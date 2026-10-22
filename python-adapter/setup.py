"""Build the small standalone Rust decoder into the adapter wheel."""
import os
import platform
from pathlib import Path
import shutil
import subprocess
import sys

from setuptools import Extension, setup
from setuptools.command.build_ext import build_ext
from setuptools.command.bdist_wheel import bdist_wheel


class BuildDecoder(build_ext):
    def build_extension(self, ext):
        root = Path(__file__).resolve().parent
        target = Path(self.build_temp).resolve() / "cargo"
        env = dict(os.environ)
        # Avoid compiling against a lab or parent project's Cargo target directory.
        env["CARGO_TARGET_DIR"] = str(target)
        subprocess.run([
            "cargo", "build", "--release", "--locked",
            "--manifest-path", str(root / "native/Cargo.toml"),
        ], check=True, env=env)
        name = ("mzlab_decoder.dll" if sys.platform == "win32" else
                "libmzlab_decoder.dylib" if sys.platform == "darwin" else
                "libmzlab_decoder.so")
        dest = Path(self.get_ext_fullpath(ext.name))
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(target / "release" / name, dest)


class PlatformWheel(bdist_wheel):
    def get_tag(self):
        python, abi, plat = super().get_tag()
        if sys.platform == "darwin":
            # A universal Python can build only the host Rust architecture.
            plat = plat.replace("universal2", platform.machine())
        return python, abi, plat


setup(ext_modules=[Extension("mzpeak_compat._decoder", sources=[])],
      cmdclass={"build_ext": BuildDecoder, "bdist_wheel": PlatformWheel})
