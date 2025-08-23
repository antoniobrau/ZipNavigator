# examples/example_resume.py
import os
import tempfile
import zipfile
from zipnavigator import ZipNavigator

def make_sample_zip(path):
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("data/a.txt", "A\n")
        z.writestr("data/b.txt", "B\n")
        z.writestr("data/c.txt", "C\n")
        z.writestr("docs/readme.md", "# readme\n")

def main():
    with tempfile.TemporaryDirectory() as td:
        zpath = os.path.join(td, "dataset.zip")
        extract_subdir = "extracted_zip"
        make_sample_zip(zpath)

        # Prima "sessione": un batch
        nav = ZipNavigator(zpath)
        nav.initialize_iterator(
            output_dir=td,
            batch_size=2,
            extract_subdir=extract_subdir,
            reset=True,
            seed=7,
            extensions=[".txt", ".md"],
            on_error="skip",
            max_retries=0,
            validate_crc=False,
        )
        first_batch = next(nav)
        print("First batch:", [os.path.relpath(p, os.path.join(td, extract_subdir)) for p in first_batch])

        # Simula chiusura (senza salvare niente altrove: lo stato è nella cartella di estrazione)
        nav.close()

        # Seconda "sessione": resume e completa
        nav2 = ZipNavigator(zpath)
        nav2.resume_iterator(output_dir=td, extract_subdir=extract_subdir)
        for batch in nav2:
            print("Resumed batch:", [os.path.relpath(p, os.path.join(td, extract_subdir)) for p in batch])

        st = nav2.iterator_status()
        print("Final status:", st)

if __name__ == "__main__":
    main()
