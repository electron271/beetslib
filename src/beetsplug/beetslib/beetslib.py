import os
from pathlib import Path
import subprocess
from beets import ui
from beets.library import Album, Library
from beets.plugins import BeetsPlugin
from beets.ui import Subcommand
from multiprocessing.pool import ThreadPool


class BeetsLib(BeetsPlugin):
    def __init__(self):
        super().__init__()
        self.opusdir = Path(
            self.config["opusdir"].get(str)
            if self.config["opusdir"].get(str) is not None
            else "/root/Music"
        )
        self.pool = ThreadPool()

        if (
            not self.opusdir.is_dir()
        ):  # im just doing this instead of exists() to raise an error if its a file
            self.opusdir.mkdir(parents=False, exist_ok=True)

    def _flac_to_opus(self, flac_file, opus_file):
        self._log.info(f"converting {flac_file} to {opus_file}")
        subprocess.run(
            [
                "opusenc",
                "--bitrate",
                "128",
                "--vbr",
                "--ignorelength",
                flac_file,
                opus_file,
            ]
        )
        self._log.info(f"done converting {flac_file} to {opus_file}")

    def _replaygain_album(self, files):
        subprocess.run(
            ["rsgain", "custom", "--album", "--tagmode=i", "--opus-mode=s", *files]
        )

    def _process_album(self, album: Album):
        self._log.info(f"processing album: {album.title}")
        tracks = album.items()

        self._log.info(f"calculating replaygain for album: {album.title}")
        replaygain = self.pool.apply_async(
            self._replaygain_album, ([str(track.path) for track in tracks],)
        )

        starmap = []
        for track in tracks:
            if track.format != "FLAC":  # TODO: add better handling for this probably
                raise ValueError(f"track {track.title} isnt a flac")

            starmap.append(
                (
                    track.path,
                    Path(track.destination(basedir=self.opusdir).decode()).with_suffix(
                        ".opus"
                    ),
                )
            )

        self.pool.starmap(self._flac_to_opus, starmap)

        self._log.info(f"calculating replaygain for converted album: {album.title}")
        self._replaygain_album([str(opus_file) for _, opus_file in starmap])

        if not replaygain.ready():
            replaygain.wait()

        self._log.info(f"done processing album: {album.title}")

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
        self.pool.map(self._process_album, albums)

        ui.print_("done")
