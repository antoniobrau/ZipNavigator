# examples/example_quickstart.py
from zipnavigator import ZipNavigator

# Open an existing ZIP
with ZipNavigator("bundle.zip") as nav:
    # List root entries
    print("Root:", nav.ls())

    # Change directory
    nav.cd("payload/")
    print("Inside payload:", nav.ls())

    # Read a file as text
    print("data1.csv contents:")
    print(nav.cat("data1.csv"))

    # Extract all .csv files in batches of 5
    nav.initialize_iterator(
        output_dir="out",
        batch_size=5,
        extensions=[".csv"],
    )
    for batch in nav:
        print("Extracted batch:", batch)
