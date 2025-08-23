
import os
import tempfile
import zipfile
from zipnavigator import ZipNavigator

def make_sample_zip(path: str) -> None:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        # CSV nella sottocartella "payload/"
        z.writestr("payload/data1.csv", "a,b,c\n1,2,3\n")
        z.writestr("payload/data2.csv", "x,y,z\n4,5,6\n")
        # altri file fuori
        z.writestr("top.txt", "hello\n")
        z.writestr("docs/readme.md", "# readme\n")

def main():
    with tempfile.TemporaryDirectory() as td:
        zpath = os.path.join(td, "bundle.zip")
        make_sample_zip(zpath)

        # Usa SEMPRE il context manager per evitare handle aperti su Windows
        with ZipNavigator(zpath) as nav:
            # Diagnostica: mostra root e contenuto ricorsivo
            print("PWD:", nav.pwd())
            print("Root:", nav.ls())
            all_entries = nav.ls(recursive=True)
            print("All entries:", all_entries)

            # Imposta la base in 'payload/' per limitare la vista ai soli CSV
            nav.cd("payload/")
            print("PWD after cd:", nav.pwd())
            print("Here:", nav.ls())

            # Avvio iteratore: cerchiamo SOLO .csv
            try:
                nav.initialize_iterator(
                    output_dir=td,
                    batch_size=10,
                    extract_subdir="batch",
                    reset=True,
                    seed=42,
                    extensions=[".csv"],   # <-- coerente con lo ZIP creato sopra
                    on_error="skip",
                    max_retries=0,
                    validate_crc=True,
                )
            except RuntimeError as e:
                # Diagnostica amichevole per capire perché non ha trovato nulla
                print("Initialize failed:", e)
                print("TIP: verifica PWD, ls(recursive=True) e il filtro 'extensions'.")
                print("PWD was:", nav.pwd())
                print("Entries here:", nav.ls(recursive=True))
                return

            # Consuma i batch
            for batch in nav:
                rels = [os.path.relpath(p, os.path.join(td, "batch")) for p in batch]
                print("Batch:", rels)

            st = nav.iterator_status()
            print("Final status:", st)

if __name__ == "__main__":
    main()
