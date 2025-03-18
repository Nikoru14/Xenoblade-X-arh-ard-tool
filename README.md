Below is the README translated into GitHub Markdown format, with all sections preserved:

```md
# XBC1 and ARD/ARH Archiver

A tool for working with Xenoblade Chronicles file formats: **XBC1, ARD, and ARH**.

## Features
- Extracts XBC1 files
- Packs files into XBC1 format
- Extracts ARD archives using ARH files
- Creates new ARD/ARH archives
- Filters files by type (e.g., only BDAT)
- Multithreaded processing for high performance

## Installation

### Install dependencies
```bash
pip install zstandard tqdm
```

## Usage

### Extract an XBC1 file
```bash
python xbc1_tool.py input.xbc1 [output_file]
```

### Pack a file into XBC1 format
```bash
python xbc1_tool.py input.bin -c [output_file]
```

### Extract an ARD archive

#### Extract all files
```bash
python xbc1_tool.py --ard game.ard game.arh [output_directory]
```

#### Extract only BDAT files
```bash
python xbc1_tool.py --ard game.ard game.arh [output_directory] --only-bdat
```

### Create an ARD archive

#### Create an archive without file compression
```bash
python xbc1_tool.py --create-ard input_directory output.ard output.arh
```

#### Create an archive with file compression
```bash
python xbc1_tool.py --create-ard input_directory output.ard output.arh --compress-files
```
