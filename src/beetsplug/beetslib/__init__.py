import os
from pathlib import Path
import subprocess
from beets import ui
from beets.dbcore import FieldQuery
from beets.library import Album, Item, Library
from beets.plugins import BeetsPlugin
from beets.ui import Subcommand
from multiprocessing.pool import AsyncResult, ThreadPool


class BeetsLib(BeetsPlugin):
    def __init__(self):
        super().__init__()
        self.config.add({"opusdir": "/root/Music"})

        self.opusdir = Path(self.config["opusdir"].get(str))
        self.pool = ThreadPool(processes=(os.cpu_count() or 1) * 4)

        if (
            not self.opusdir.is_dir()
        ):  # im just doing this instead of exists() to raise an error if its a file
            self.opusdir.mkdir(parents=False, exist_ok=True)

    def _flac_to_opus(self, flac_file: Path, opus_file: Path, quiet: bool = False):
        self._log.info(
            f"converting {flac_file} to {opus_file}"
        ) if not quiet else self._log.debug(f"converting {flac_file} to {opus_file}")
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
        self._log.info(
            f"done converting {flac_file} to {opus_file}"
        ) if not quiet else self._log.debug(
            f"done converting {flac_file} to {opus_file}"
        )

    def _replaygain_album(self, files, album_name, quiet: bool = False):
        self._log.info(
            f"calculating replaygain for album: {album_name}"
        ) if not quiet else self._log.debug(
            f"calculating replaygain for album: {album_name}"
        )
        subprocess.run(
            ["rsgain", "custom", "--album", "--tagmode=i", "--opus-mode=s", *files],
            capture_output=False,
        )
        self._log.info(
            f"done calculating replaygain for album: {album_name}"
        ) if not quiet else self._log.debug(
            f"done calculating replaygain for album: {album_name}"
        )

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
        singletons = [
            i for i in lib.items() if i.album is None
        ]  # shit ass solution to an issue i didnt see coming

        ui.print_("converting library...")
        needs_update_results: list[tuple[Album | Item, AsyncResult]] = []
        needs_replaygain_results: list[tuple[Album | Item, AsyncResult]] = []
        results: list[AsyncResult] = []

        for album in albums:
            tracks = album.items()

            needs_update_results.append(
                (
                    album,
                    self.pool.apply_async(
                        self._replaygain_album,
                        (
                            [str(track.filepath) for track in tracks],
                            album.album,
                        ),
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

        for singleton in singletons:
            needs_update_results.append(
                (
                    singleton,
                    self.pool.apply_async(
                        self._replaygain_album,
                        (
                            [singleton.filepath],
                            singleton.filepath.name,
                        ),
                    ),
                )
            )

            if track.format != "FLAC":  # TODO: add better handling for this probably
                raise ValueError(f"track {track.filepath} isnt a flac")

            self._log.debug(f"processing track: {track.filepath}")
            needs_replaygain_results.append(
                (
                    album,
                    self.pool.apply_async(
                        self._flac_to_opus,
                        (
                            track.filepath,
                            Path(
                                track.destination(
                                    basedir=self.opusdir.__bytes__()
                                ).decode()
                            ).with_suffix(".opus"),
                        ),
                    ),
                )
            )

        for album, result in needs_replaygain_results:
            tracks = []
            if isinstance(album, Album):
                tracks = [
                    Path(
                        track.destination(basedir=self.opusdir.__bytes__()).decode()
                    ).with_suffix(".opus")
                    for track in album.items()
                ]
            elif isinstance(album, Item):
                tracks = [
                    Path(
                        album.destination(basedir=self.opusdir.__bytes__()).decode()
                    ).with_suffix(".opus")
                ]

            result.wait()
            results.append(
                self.pool.apply_async(
                    self._replaygain_album,
                    (
                        tracks,
                        album.album or album.filepath.name,
                    ),
                )
            )

        for album, result in needs_update_results:
            result.wait()
            results.append(self.pool.apply_async(album.store, ()))

        for result in results:
            result.wait()

        self.pool.map(lambda x: x.store(), albums)

        ui.print_("done")
