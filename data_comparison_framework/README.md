# Data Comparison Framework

This is a modern, feature-rich data comparison framework built with Python and Streamlit. It supports multiple data sources and targets including CSV, DAT, Parquet, zipped flat files, SQL Server, Teradata, stored procedures, and APIs. The framework provides advanced column mapping, data type handling, and generates comprehensive comparison reports.

## Features

- Multi-source and target data loading with support for large files
- Auto and manual column mapping with data type overrides
- Multiple join columns selection
- Column exclusion from comparison
- User-defined delimiters for flat files
- Side-by-side difference Excel report with pass/fail and color highlights
- Ydata profiling and DataCompy comparison reports
- Excel regression report with aggregation, count, and distinct checks
- Download individual reports or all reports as a zipped archive
- Modern Streamlit UI with clear workflow and error handling

## Installation

1. Clone the repository:

```bash
git clone <repository-url>
cd data_comparison_framework
```

2. Create and activate a virtual environment (optional but recommended):

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

## Usage

Run the Streamlit app:

```bash
streamlit run app.py
```

Open the URL shown in the terminal (usually http://localhost:8501) in your browser.

## Project Structure

- `app.py`: Main Streamlit application
- `data_loader.py`: Data loading utilities
- `mapping_manager.py`: Column mapping and data type handling
- `report_generator_new.py`: Report generation functions
- `utils.py`: Utility functions for logging, file handling, etc.
- `config.py`: Configuration constants
- `requirements.txt`: Python dependencies

## Notes

- Ensure you have Python 3.8+ installed.
- For large files, sufficient memory and disk space are recommended.
- Customize `config.py` for your environment and preferences.

## License

MIT License
