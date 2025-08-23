# examples/example_resume.py
import os
import tempfile
import zipfile
from zipnavigator import ZipNavigator

def make_sample_zip(path: str) -> None:
    # Always use a context manager when writing the zip
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("data/a.txt", "A\n")
        z.writestr("data/b.txt", "B\n")
        z.writestr("data/c.txt", "C\n")
        z.writestr("docs/readme.md", "# readme\n")

def _rel(out_root: str, subdir: str, abs_path: str) -> str:
    return os.path.relpath(abs_path, os.path.join(out_root, subdir)).replace(os.sep, "/")

def main():
    with tempfile.TemporaryDirectory() as td:
        zpath = os.path.join(td, "dataset.zip")
        extract_subdir = "extracted_zip"
        make_sample_zip(zpath)  # zip is fully closed here

        # --- Session 1: initialize and extract a first batch ---
        with ZipNavigator(zpath) as nav:
            # Optional: narrow base to 'data/' only
            nav.cd("data/")
            nav.initialize_iterator(
                output_dir=td,
                batch_size=2,           # multiple batches to demonstrate resume
                extract_subdir=extract_subdir,
                reset=True,
                seed=7,
                extensions=[".txt", ".md"],
                on_error="skip",
                max_retries=0,
                validate_crc=False,
            )
            first_batch = next(nav)
            print("First batch:", [_rel(td, extract_subdir, p) for p in first_batch])

            st = nav.iterator_status()
            print("State file:", st["state_file"])

        # At this point nav is CLOSED; no handle remains open on dataset.zip

        # --- Session 2: resume and finish ---
        with ZipNavigator(zpath) as nav2:
            nav2.resume_iterator(output_dir=td, extract_subdir=extract_subdir)
            for batch in nav2:
                print("Resumed batch:", [_rel(td, extract_subdir, p) for p in batch])
            print("Final status:", nav2.iterator_status())

if __name__ == "__main__":
    main()
