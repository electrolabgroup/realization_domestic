from cx_Freeze import setup, Executable
import sys

# Define the base to be "Win32GUI" if your application does not open a console window
base = None
if sys.platform == "win32":
    base = "Win32GUI"  # For GUI applications, change to None if you want a console window

# Define the files to include (if any)
files = [("ELEC.png", "build/exe.win-amd64-3.9/ELEC.png")]  # Modify if you have additional files

# Define your application's setup
setup(
    name="Electro Labs",  # Name of your application
    version="1.0",
    description="C&R Updater Application",
    options={"build_exe": {"include_files": files}},
    executables=[Executable("Duplicater_DF.py", base=base, target_name="Electro_Labs.exe")]
)
