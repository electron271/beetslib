import os
from pathlib import Path
import subprocess
from beets import ui
from beets.library import Album, Library
from beets.plugins import BeetsPlugin
from beets.ui import Subcommand
from multiprocessing.pool import AsyncResult, ThreadPool


class BeetsLib(BeetsPlugin):
    def __init__(self):
        super().__init__()
        self.config.add({"opusdir": "/root/Music"})

        self.opusdir = Path(self.config["opusdir"].get(str))
        self.pool = ThreadPool(processes=(os.cpu_count() or 1) * 2)

        if (
            not self.opusdir.is_dir()
        ):  # im just doing this instead of exists() to raise an error if its a file
            self.opusdir.mkdir(parents=False, exist_ok=True)

    def _flac_to_opus(self, flac_file: Path, opus_file: Path):
        self._log.info(f"converting {flac_file} to {opus_file}")
        if not opus_file.parent.exists():
            opus_file.parent.mkdir(parents=True, exist_ok=True)

        subprocess.run(
            [
                "opusenc",
                "--bitrate",
                "128",
                "--vbr",
                "--ignorelength",
                flac_file,
                opus_file,
            ],
            capture_output=True,
        )
        self._log.info(f"done converting {flac_file} to {opus_file}")

    def _replaygain_album(self, files, album_name):
        self._log.info(f"calculating replaygain for album: {album_name}")
        subprocess.run(
            ["rsgain", "custom", "--album", "--tagmode=i", "--opus-mode=s", *files],
            capture_output=True,
        )
        self._log.info(f"done calculating replaygain for album: {album_name}")

    def commands(self):
        reconvert = Subcommand(
            "reconvert",
            help="reconverts and reanalyzes replaygain metadata",
        )
        reconvert.func = self.reconvert
        return [reconvert]

    def reconvert(self, lib: Library, opts, args):
        opus_files = [f for f in os.listdir(self.opusdir) if not f.startswith(".")]
        if opus_files:
            ui.print_(f'"{self.opusdir}" is not empty')
            return

        albums = lib.albums()

        ui.print_("converting albums...")
        results = []
        needs_replaygain_results: list[tuple[Album, AsyncResult]] = []
        for album in albums:
            tracks = album.items()

            results.append(
                self.pool.apply_async(
                    self._replaygain_album,
                    (
                        [str(track.filepath) for track in tracks],
                        album.album,
                    ),
                )
            )

            starmap = []
            for track in tracks:
                if (
                    track.format != "FLAC"
                ):  # TODO: add better handling for this probably
                    raise ValueError(f"track {track.filepath} isnt a flac")

                self._log.debug(f"processing track: {track.filepath}")
                starmap.append(
                    (
                        track.filepath,
                        Path(
                            track.destination(basedir=self.opusdir.__bytes__()).decode()
                        ).with_suffix(".opus"),
                    )
                )
            needs_replaygain_results.append(
                (album, self.pool.starmap_async(self._flac_to_opus, starmap))
            )

        for album, result in needs_replaygain_results:
            result.wait()
            self._replaygain_album(
                [str(opus_file) for _, opus_file in starmap], album.album
            )

        for result in results:
            result.wait()

        ui.print_("done")
