# examples/example_iterator.py
import os
import tempfile
import zipfile
from zipnavigator import ZipNavigator

def make_sample_zip(path):
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        # subdir payload con csv
        z.writestr("payload/data1.csv", "a,b,c\n1,2,3\n")
        z.writestr("payload/data2.csv", "x,y,z\n4,5,6\n")
        # altri file fuori
        z.writestr("top.txt", "hello\n")
        z.writestr("docs/readme.md", "# readme\n")

def main():
    with tempfile.TemporaryDirectory() as td:
        zpath = os.path.join(td, "bundle.zip")
        make_sample_zip(zpath)

        nav = ZipNavigator(zpath)

        # Limita il contesto alla subdir "payload/"
        nav.cd("payload/")

        nav.initialize_iterator(
            output_dir=td,
            batch_size=10,
            extract_subdir="batch",
            reset=True,
            seed=42,
            extensions=[".csv"],
            on_error="skip",
            max_retries=1,
            validate_crc=True,  # integrità (più lento ma sicuro)
        )

        for batch in nav:
            print("Batch:", [os.path.relpath(p, os.path.join(td, "batch")) for p in batch])

        st = nav.iterator_status()
        print("Status:", st)

if __name__ == "__main__":
    main()
