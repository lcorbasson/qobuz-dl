import collections
import logging
import os
import subprocess
from typing import Tuple

import requests
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from pathvalidate import sanitize_filename, sanitize_filepath
from tqdm import tqdm
import datetime
import time

import qobuz_dl.metadata as metadata
from qobuz_dl.color import OFF, GREEN, RED, YELLOW, CYAN
from qobuz_dl.config import USER_AGENT
from qobuz_dl.exceptions import DownloadError, Ignored, NonStreamable

QL_DOWNGRADE = "FormatRestrictedByFormatAvailability"
# used in case of error
DEFAULT_FORMATS = {
    "MP3": [
        "{artist} - {album} ({year}) [MP3]",
        "{tracknumber}. {tracktitle}",
    ],
    "Unknown": [
        "{artist} - {album}",
        "{tracknumber}. {tracktitle}",
    ],
}

DEFAULT_FOLDER_FORMAT = "{artist} - {album} ({year}) [{bit_depth}B-{sampling_rate}kHz]"
DEFAULT_TRACK_FORMAT = "{tracknumber}. {tracktitle}"

logger = logging.getLogger(__name__)


class Download:
    def __init__(
        self,
        client,
        item_id: str,
        path: str,
        quality: int,
        embed_art: bool = False,
        albums_only: bool = False,
        downgrade_quality: bool = False,
        cover_og_quality: bool = False,
        no_cover: bool = False,
        folder_format=None,
        track_format=None,
        dry_run=False,
        verbose=False,
    ):
        self.client = client
        self.item_id = item_id
        self.path = path
        self.quality = quality
        self.albums_only = albums_only
        self.embed_art = embed_art
        self.downgrade_quality = downgrade_quality
        self.cover_og_quality = cover_og_quality
        self.no_cover = no_cover
        self.folder_format = folder_format or DEFAULT_FOLDER_FORMAT
        self.track_format = track_format or DEFAULT_TRACK_FORMAT
        self.dry_run = dry_run
        self.verbose = verbose

        if verbose:
            logger.setLevel(logging.DEBUG)

    def download_id_by_type(self, track=True):
        if not track:
            self.download_release()
        else:
            self.download_track()

    def download_release(self):
        count = 0
        meta = self.client.get_album_meta(self.item_id)

        try:
            if not meta.get("streamable"):
                raise NonStreamable("This release is not streamable")

            if self.albums_only and (
                meta.get("release_type") != "album"
                or meta.get("artist").get("name") == "Various Artists"
            ):
                raise Ignored(f'{OFF}Ignoring Single/EP/VA: {meta.get("title", "n/a")}')

            album_title = _get_title(meta)

            format_info = self._get_format(meta)
            file_format, quality_met, bit_depth, sampling_rate = format_info

            if not self.downgrade_quality and not quality_met:
                raise Ignored(
                    f"{OFF}Skipping {album_title} as it doesn't meet quality requirement"
                )

            url = meta.get("url")

            logger.info(
                f"\n{YELLOW}Downloading: {album_title}\nQuality: {file_format}"
                f" ({bit_depth}/{sampling_rate})\n"
                f"{url}"
            )
            if not self.dry_run:
                self.client.trace_meta(self.path, "album", self.item_id, meta)

        except Exception as e:
            if isinstance(e, Ignored):
                logger.info(e)
                return
            else:
                raise e

        album_attr = self._get_album_attr(
            meta, album_title, file_format, bit_depth, sampling_rate
        )
        folder_format, track_format = _clean_format_str(
            self.folder_format, self.track_format, file_format
        )
        sanitized_album_attr = {
            attr: sanitize_filename(str(value)) for attr, value in album_attr.items()
        }
        sanitized_title = sanitize_filepath(folder_format.format(**sanitized_album_attr))
        dirn = os.path.join(self.path, sanitized_title)
        logger.debug(f"{OFF+YELLOW}Release folder: {dirn}")
        os.makedirs(dirn, exist_ok=True)

        if self.no_cover:
            logger.info(f"{OFF}Skipping cover")
        elif not self.dry_run:
            _get_extra(meta["image"]["large"], dirn, og_quality=self.cover_og_quality)

        if "goodies" in meta:
            try:
                if not self.dry_run:
                    _get_extra(meta["goodies"][0]["url"], dirn, "booklet.pdf")
            except:  # noqa
                pass
        media_numbers = [track["media_number"] for track in meta["tracks"]["items"]]
        track_count = len(meta['tracks']['items'])
        media_count = len([*{*media_numbers}])
        is_multiple = True if media_count > 1 else False
        logger.info(
            f"{YELLOW}{track_count} tracks in {media_count} media"
        )
        last_errors = []
        try:
            for i in meta["tracks"]["items"]:
                if not i.get("streamable"):
                    logger.info(f"{OFF}{i.get('title', 'Track')} is not streamable. Skipping")
                    count = count + 1
                    continue
                parse = self.client.get_track_url(i["id"], fmt_id=self.quality)
                if "sample" not in parse and parse["sampling_rate"]:
                    is_mp3 = True if int(self.quality) == 5 else False
                    try:
                        self._download_and_tag(
                            dirn,
                            count,
                            parse,
                            i,
                            meta,
                            False,
                            is_mp3,
                            i["media_number"] if is_multiple else None,
                        )
                    except Exception as e:
                        logger.warning(e)
                        last_errors.append(e)
                else:
                    logger.info(f"{OFF}Demo. Skipping")
                count = count + 1
        finally:
            if len(last_errors) > 0:
                logger.error(
                    f"{RED}Errors encountered while downloading {album_title}:"
                    + '\n  - '
                    + '\n  - '.join(str(e) for e in last_errors)
                )
                raise last_errors[0]
        logger.info(f"{GREEN}Completed")

    def download_track(self):
        meta = self.client.get_track_meta(self.item_id)
        if not meta.get("streamable"):
            logger.info(f"{OFF}Track is not streamable. Skipping")
            logger.info(f"{GREEN}Completed")
            return

        parse = self.client.get_track_url(self.item_id, fmt_id=self.quality)

        if "sample" not in parse and parse["sampling_rate"]:
            track_title = _get_title(meta)
            artist = _safe_get(meta, "performer", "name")
            logger.info(f"\n{YELLOW}Downloading: {artist} - {track_title}")
            format_info = self._get_format(meta, is_track_id=True, track_url_dict=parse)
            file_format, quality_met, bit_depth, sampling_rate = format_info

            folder_format, track_format = _clean_format_str(
                self.folder_format, self.track_format, str(bit_depth)
            )

            if not self.downgrade_quality and not quality_met:
                logger.info(
                    f"{OFF}Skipping {track_title} as it doesn't "
                    "meet quality requirement"
                )
                return
            track_attr = self._get_track_attr(
                meta, track_title, bit_depth, sampling_rate
            )
            sanitized_title = sanitize_filepath(folder_format.format(**track_attr))

            dirn = os.path.join(self.path, sanitized_title)
            logger.debug(f"{OFF+YELLOW}Track folder: {dirn}")
            os.makedirs(dirn, exist_ok=True)
            if self.no_cover:
                logger.info(f"{OFF}Skipping cover")
            elif not self.dry_run:
                _get_extra(
                    meta["album"]["image"]["large"],
                    dirn,
                    og_quality=self.cover_og_quality,
                )
            is_mp3 = True if int(self.quality) == 5 else False
            self._download_and_tag(
                dirn,
                1,
                parse,
                meta,
                meta,
                True,
                is_mp3,
                False,
            )
        else:
            logger.info(f"{OFF}Demo. Skipping")
        logger.info(f"{GREEN}Completed")

    def _download_and_tag(
        self,
        root_dir,
        tmp_count,
        track_url_dict,
        track_metadata,
        album_or_track_metadata,
        is_track,
        is_mp3,
        multiple=None,
    ):
        extension = ".mp3" if is_mp3 else ".flac"

        if "url" not in track_url_dict and "url_template" not in track_url_dict:
            logger.info(f"{OFF}Track not available for download")
            return
        try:
            url = track_url_dict["url"]
        except KeyError:
            url = track_url_dict["url_template"]

        if multiple:
            root_dir = os.path.join(root_dir, f"Disc {multiple}")
            if not self.dry_run:
                os.makedirs(root_dir, exist_ok=True)

        filename = os.path.join(root_dir, f".{tmp_count:02}.tmp")

        # Determine the filename
        track_title = track_metadata.get("title")
        # different versions, remixes, etc will differ with an additional version attribute
        # we need to attach this to the regular title if and only if it exists.
        version = track_metadata.get("version")
        if version:
            track_title = track_title + " (" + version + ")"
        artist = _safe_get(track_metadata, "performer", "name")
        filename_attr = self._get_filename_attr(artist, track_metadata, track_title)

        # track_format is a format string
        # e.g. '{tracknumber}. {artist} - {tracktitle}'
        formatted_path = sanitize_filename(self.track_format.format(**filename_attr))
        final_file = os.path.join(root_dir, formatted_path)[:250] + extension

        if os.path.isfile(final_file):
            logger.info(f"{OFF}{track_title} was already downloaded")
            return

        logger.debug(f"{OFF+YELLOW}Track will be saved to: {final_file}")

        track_duration = track_metadata.get("duration")

        max_retries = 5
        last_error = None
        # Try with the normal mode first, then try using segments
        attempts = [
            {
                'force_segments_mode': force_segments_mode,
                'mode_retries': mode_retries,
            }
            for force_segments_mode in (False, True)
                for mode_retries in range(max_retries)
        ]
        for attempt in attempts:
            force_segments_mode = attempt['force_segments_mode']
            mode_retries = attempt['mode_retries']
            if mode_retries > 0:
                wait = 2 ** mode_retries  # 2, 4, 8, 16 seconds
                logger.warning(
                    f"{YELLOW}Network error, retrying in {wait}s "
                    f"(attempt {mode_retries + 1}/{max_retries})..."
                )
                time.sleep(wait)
                if os.path.isfile(filename):
                    os.remove(filename)
                # Re-fetch a fresh download URL — the CDN rejects reused/stale URLs
                try:
                    track_url_dict = self.client.get_track_url(
                        track_metadata["id"],
                        fmt_id=self.quality,
                        force_segments_mode=force_segments_mode,
                    )
                    try:
                        url = track_url_dict["url"]
                    except KeyError:
                        url = track_url_dict["url_template"]
                    logger.debug(f"{OFF+YELLOW}Retrying \"{final_file}\" with URL: {url}")
                except Exception as url_err:
                    logger.warning(f"{YELLOW}Could not refresh URL: {url_err}")
                time.sleep(wait)
            try:
                if self.dry_run:
                    logger.info(f"{OFF}{track_title} won't be downloaded from {url}")
                    return

                tqdm_download(track_url_dict, filename, filename, duration=track_duration)
                break
            except (
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                ConnectionError,
                OSError,
            ) as e:
                last_error = e
                logger.warning(
                    f"{YELLOW}Download attempt {mode_retries + 1} failed: {str(e)}"
                )
                if os.path.isfile(final_file):
                    logger.info(
                        f"{GREEN}File \"{final_file}\" was injected, using it"
                    )
                    os.rename(final_file, filename)
                    break
            if not force_segments_mode:
                logger.warning(
                    f"{YELLOW}Failed to download {track_title} after {max_retries} "
                    f"attempts (CDN issue). Retrying with a segments-based download."
                )
            else:
                logger.warning(
                    f"{YELLOW}Failed to download {track_title} after {max_retries} "
                    f"attempts using segments (Web Player mode)."
                )
        else:
            logger.error(
                f"{RED}Failed to download {track_title} after trying all available modes. "
                f"Skipping track..."
            )
            if os.path.isfile(filename):
                os.remove(filename)
            if last_error:
                raise last_error
            raise DownloadError(f"{RED}Failed to download {track_title}")

        tag_function = metadata.tag_mp3 if is_mp3 else metadata.tag_flac
        try:
            tag_function(
                filename,
                root_dir,
                final_file,
                track_metadata,
                album_or_track_metadata,
                is_track,
                self.embed_art,
            )
        except Exception as e:
            logger.error(f"{RED}Error tagging the file: {e}", exc_info=True)

    @staticmethod
    def _get_filename_attr(artist, track_metadata, track_title):
        return {
            "artist": artist,
            "albumartist": _safe_get(
                track_metadata, "album", "artist", "name", default=artist
            ),
            "bit_depth": track_metadata["maximum_bit_depth"],
            "sampling_rate": track_metadata["maximum_sampling_rate"],
            "tracktitle": track_title,
            "version": track_metadata.get("version"),
            "tracknumber": f"{track_metadata['track_number']:02}",
        }

    @staticmethod
    def _get_track_attr(meta, track_title, bit_depth, sampling_rate):
        return {
            "album": meta["album"]["title"],
            "artist": meta["album"]["artist"]["name"],
            "tracktitle": track_title,
            "year": meta["album"]["release_date_original"].split("-")[0],
            "bit_depth": bit_depth,
            "sampling_rate": sampling_rate,
        }

    @staticmethod
    def _get_album_attr(meta, album_title, file_format, bit_depth, sampling_rate):
        return {
            "artist": meta["artist"]["name"],
            "albumartist": meta["artist"]["name"],
            "album": album_title,
            "year": meta["release_date_original"].split("-")[0],
            "format": file_format,
            "bit_depth": bit_depth,
            "sampling_rate": sampling_rate,
        }

    def _get_format(self, item_dict, is_track_id=False, track_url_dict=None):
        quality_met = True
        if int(self.quality) == 5:
            return ("MP3", quality_met, None, None)
        track_dict = item_dict
        if not is_track_id:
            track_dict = item_dict["tracks"]["items"][0]

        try:
            new_track_dict = (
                self.client.get_track_url(track_dict["id"], fmt_id=self.quality)
                if not track_url_dict
                else track_url_dict
            )
            restrictions = new_track_dict.get("restrictions")
            if isinstance(restrictions, list):
                if any(
                    restriction.get("code") == QL_DOWNGRADE
                    for restriction in restrictions
                ):
                    quality_met = False

            return (
                "FLAC",
                quality_met,
                new_track_dict["bit_depth"],
                new_track_dict["sampling_rate"],
            )
        except (KeyError, requests.exceptions.HTTPError):
            return ("Unknown", quality_met, None, None)


def tqdm_download(url, fname, desc, duration=None, playback_speed=1.0):
    done = False
    segments = 0
    # `url` can be a string or a dict, let's sort this out
    if isinstance(url, collections.abc.Mapping):
        track_url_dict = url
        if "url" in track_url_dict:
            # A direct URL was returned by `get_track_url` (normal API mode)
            # and the full dict returned was passed to tqdm_download in the `url` parameter,
            # this is similar to the `url` parameter being a string
            url = track_url_dict["url"]
            logger.debug(f"{OFF+YELLOW}Downloading from URL: {url}")
        else:
            # Fallback: we'll download using segments (emulating the Web Player)
            url = track_url_dict["url_template"]
            segments = track_url_dict['n_segments'] + 1
            logger.debug(f"{OFF+YELLOW}Downloading {segments} segments using URL template: {url}")
    else:
        logger.debug(f"{OFF+YELLOW}Downloading from URL: {url}")

    try:
        duration_str = "≤" + str(datetime.timedelta(seconds=duration)) + f"/{playback_speed:0.1f}" 
    except TypeError:
        duration_str = ""
    tmp_fname = fname + ".mp4"
    headers = {
        'User-Agent': USER_AGENT,
    }

    try:
        tic = time.perf_counter()
        if segments == 0:
            r = requests.get(
                url,
                allow_redirects=True,
                stream=True,
                headers=headers,
                timeout=(10, 60),
            )
            r.raise_for_status()
            total = int(r.headers.get("content-length", 0))
        else:
            segment_uuid = None
            total = 0
            for segment in range(segments):
                r = requests.head(
                    url.replace("$SEGMENT$", str(segment)),
                    allow_redirects=True,
                    headers=headers,
                )
                r.raise_for_status()
                total += int(r.headers.get("content-length", 0))
                r.close()

        download_size = 0
        with open(tmp_fname, "wb") as file, tqdm(
            total=total,
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
            desc=desc,
            bar_format=CYAN + "{n_fmt}/{total_fmt} /// {desc}"
                + "        "
                + "[{elapsed}+{remaining}" + duration_str + ", {rate_fmt}{postfix}]",
        ) as bar:
            if segments == 0:
                for data in r.iter_content(chunk_size=1024):
                    r.raise_for_status()
                    size = file.write(data)
                    bar.update(size)
                    download_size += size
            else:
                for segment in range(segments):
                    r = requests.get(
                        url.replace("$SEGMENT$", str(segment)),
                        allow_redirects=True,
                        stream=True,
                        headers=headers,
                    )
                    r.raise_for_status()
                    segment_total = int(r.headers.get("content-length", 0))
                    segment_size = 0
                    segment_data = bytearray()
                    for data in r.iter_content(chunk_size=1024):
                        r.raise_for_status()
                        segment_data.extend(data)
                        size = len(data)
                        bar.update(size)
                        segment_size += size
                    download_size += segment_size
                    r.close()

                    if segment_total and segment_total != segment_size:
                        raise ConnectionError("File download was interrupted for " + fname)
                    if segment == 1:
                        segment_uuid = _get_qobuz_segment_uuid(segment_data)
                        if segment_uuid is None:
                            raise requests.exceptions.ConnectionError(
                                "Cannot find Qobuz segment UUID for " + fname
                            )
                    file.write(
                        _decrypt_qobuz_segment(
                            segment_data, track_url_dict["raw_key"], segment_uuid
                        )
                    )

        toc = time.perf_counter()
        elapsed_time = (toc - tic)
        if duration is not None:
            waiting_time = max(0, duration / playback_speed - elapsed_time)
            logger.debug(f"{OFF+YELLOW}Sleeping {waiting_time:0.0f} seconds to match playback time")
            time.sleep(waiting_time)

        if segments == 0:
            if download_size < total:
                raise ConnectionError(
                    f"Incomplete download ({download_size}/{total} bytes) for {fname}"
                )
            os.rename(tmp_fname, fname)
        else:
            remux = subprocess.run(["ffmpeg", "-nostdin", "-v", "error", "-y", "-i", tmp_fname, "-c:a", "copy", "-f", "flac", fname], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
            if remux.returncode != 0:
                raise requests.exceptions.ConnectionError(
                    "File remux failed for {}: {}".format(
                        fname, remux.stderr.strip() or "ffmpeg exited with an error"
                    )
                )
        done = True

    finally:
        if r:
            r.close()
        if os.path.isfile(tmp_fname):
            os.remove(tmp_fname)
        return done


def _get_qobuz_segment_uuid(segment_data):
    pos = 0
    while pos + 24 <= len(segment_data):
        size = int.from_bytes(segment_data[pos : pos + 4], "big")
        if size <= 0 or pos + size > len(segment_data):
            break

        if bytes(segment_data[pos + 4 : pos + 8]) == b"uuid":
            return bytes(segment_data[pos + 8 : pos + 24])
        pos += size
    return None


def _decrypt_qobuz_segment(segment_data, raw_key, segment_uuid):
    if segment_uuid is None:
        return bytes(segment_data)

    buf = bytearray(segment_data)
    pos = 0
    while pos + 8 <= len(buf):
        size = int.from_bytes(buf[pos : pos + 4], "big")
        if size <= 0 or pos + size > len(buf):
            break

        if (
            bytes(buf[pos + 4 : pos + 8]) == b"uuid"
            and bytes(buf[pos + 8 : pos + 24]) == segment_uuid
        ):
            pointer = pos + 28
            data_end = pos + int.from_bytes(buf[pointer : pointer + 4], "big")
            pointer += 4
            counter_len = buf[pointer]
            pointer += 1
            frame_count = int.from_bytes(buf[pointer : pointer + 3], "big")
            pointer += 3

            for _ in range(frame_count):
                frame_len = int.from_bytes(buf[pointer : pointer + 4], "big")
                pointer += 6
                flags = int.from_bytes(buf[pointer : pointer + 2], "big")
                pointer += 2
                frame_start = data_end
                frame_end = frame_start + frame_len
                data_end = frame_end

                if flags:
                    counter = bytes(buf[pointer : pointer + counter_len]) + (
                        b"\x00" * (16 - counter_len)
                    )
                    decryptor = Cipher(
                        algorithms.AES(raw_key), modes.CTR(counter)
                    ).decryptor()
                    buf[frame_start:frame_end] = decryptor.update(
                        bytes(buf[frame_start:frame_end])
                    ) + decryptor.finalize()
                pointer += counter_len
        pos += size
    return bytes(buf)


def _get_description(item: dict, track_title, multiple=None):
    downloading_title = f"{track_title} "
    f'[{item["bit_depth"]}/{item["sampling_rate"]}]'
    if multiple:
        downloading_title = f"[Disc {multiple}] {downloading_title}"
    return downloading_title


def _get_title(item_dict):
    album_title = item_dict["title"]
    version = item_dict.get("version")
    if version:
        album_title = (
            f"{album_title} ({version})"
            if version.lower() not in album_title.lower()
            else album_title
        )
    return album_title


def _get_extra(item, dirn, extra="cover.jpg", og_quality=False):
    extra_file = os.path.join(dirn, extra)
    if os.path.isfile(extra_file):
        logger.info(f"{OFF}{extra} was already downloaded")
        return
    tqdm_download(
        item.replace("_600.", "_org.") if og_quality else item,
        extra_file,
        extra,
    )


def _clean_format_str(folder: str, track: str, file_format: str) -> Tuple[str, str]:
    """Cleans up the format strings, avoids errors
    with MP3 files.
    """
    final = []
    for i, fs in enumerate((folder, track)):
        if fs.endswith(".mp3"):
            fs = fs[:-4]
        elif fs.endswith(".flac"):
            fs = fs[:-5]
        fs = fs.strip()

        # default to pre-chosen string if format is invalid
        if file_format in ("MP3", "Unknown") and (
            "bit_depth" in fs or "sampling_rate" in fs
        ):
            default = DEFAULT_FORMATS[file_format][i]
            logger.error(
                f"{RED}invalid format string for format {file_format}"
                f". defaulting to {default}"
            )
            fs = default
        final.append(fs)

    return tuple(final)


def _safe_get(d: dict, *keys, default=None):
    """A replacement for chained `get()` statements on dicts:
    >>> d = {'foo': {'bar': 'baz'}}
    >>> _safe_get(d, 'baz')
    None
    >>> _safe_get(d, 'foo', 'bar')
    'baz'
    """
    curr = d
    res = default
    for key in keys:
        res = curr.get(key, default)
        if res == default or not hasattr(res, "__getitem__"):
            return res
        else:
            curr = res
    return res
