import os
import struct
import zlib
import zstandard as zstd
from enum import IntEnum
import concurrent.futures
from tqdm import tqdm

class CompType(IntEnum):
    ZLIB = 1
    ZSTD = 3

def decompress_xbc1_file(input_file: str, output_file: str = None) -> None:
    """
    Decompresses an XBC1 file
    
    Args:
        input_file: path to the file to decompress
        output_file: path to save the decompressed file (if None, it is generated automatically)
    """
    
    # Read the entire file
    with open(input_file, 'rb') as f:
        data = f.read()
    
    # Check the magic number
    if data[:4] != b'xbc1':
        raise ValueError(f"Not an XBC1 file! Magic number: {data[:4]}")
    
    # Read the header
    compression_type = struct.unpack("<I", data[4:8])[0]
    uncompressed_size = struct.unpack("<I", data[8:12])[0]
    compressed_size = struct.unpack("<I", data[12:16])[0]
    # hash = struct.unpack("<I", data[16:20])[0]  # not used
    name = data[20:48].split(b'\x00')[0].decode('ascii', errors='ignore')
    
    print(f"Compression type: {compression_type}")
    print(f"Size before decompression: {compressed_size}")
    print(f"Size after decompression: {uncompressed_size}")
    print(f"File name in header: {name}")
    
    # Get the compressed data after the header (48 bytes)
    compressed_data = data[48:48 + compressed_size]
    
    # Decompress
    if compression_type == CompType.ZLIB:
        try:
            decompressed = zlib.decompress(compressed_data)
        except zlib.error as e:
            raise ValueError(f"ZLIB decompression error: {e}")
    
    elif compression_type == CompType.ZSTD:
        try:
            dctx = zstd.ZstdDecompressor()
            decompressed = dctx.decompress(compressed_data)
        except zstd.ZstdError as e:
            raise ValueError(f"ZSTD decompression error: {e}")
    
    else:
        raise ValueError(f"Unknown compression type: {compression_type}")
    
    # Check the size
    if len(decompressed) != uncompressed_size:
        raise ValueError(f"Incorrect size of decompressed data: expected {uncompressed_size}, got {len(decompressed)}")
    
    # Determine the output file name if not specified
    if output_file is None:
        base_name = os.path.splitext(input_file)[0]
        output_file = f"{base_name}.dec"
    
    # Save the decompressed data
    with open(output_file, 'wb') as f:
        f.write(decompressed)
    
    print(f"Decompressed to: {output_file}")

def compress_xbc1_file(input_file: str, output_file: str = None, compression_type: CompType = CompType.ZLIB, name: str = "") -> None:
    """
    Compresses a file into XBC1 format
    
    Args:
        input_file: path to the file to compress
        output_file: path to save the compressed file (if None, it is generated automatically)
        compression_type: compression type (ZLIB or ZSTD)
        name: file name to write in the header (maximum 27 characters)
    """
    
    # Read the source file
    with open(input_file, 'rb') as f:
        data = f.read()
    
    # Compress the data
    if compression_type == CompType.ZLIB:
        try:
            compressed = zlib.compress(data, level=9)  # maximum compression
        except zlib.error as e:
            raise ValueError(f"ZLIB compression error: {e}")
    
    elif compression_type == CompType.ZSTD:
        try:
            cctx = zstd.ZstdCompressor(level=22)  # maximum compression
            compressed = cctx.compress(data)
        except zstd.ZstdError as e:
            raise ValueError(f"ZSTD compression error: {e}")
    
    else:
        raise ValueError(f"Unknown compression type: {compression_type}")

    # If the name is not specified, use the input file name
    if not name:
        name = os.path.basename(input_file)
    
    # Truncate the name if it is too long (leave space for the null terminator)
    if len(name) > 27:
        name = name[:27]
    
    # Create the header
    header = bytearray()
    header.extend(b'xbc1')  # magic number
    header.extend(struct.pack("<I", compression_type))  # compression type
    header.extend(struct.pack("<I", len(data)))  # uncompressed size
    header.extend(struct.pack("<I", len(compressed)))  # compressed size
    
    # Calculate a simple hash (can be modified if a specific algorithm is needed)
    simple_hash = sum(data) & 0xFFFFFFFF
    header.extend(struct.pack("<I", simple_hash))  # hash
    
    # Add the file name (28 bytes including the null terminator)
    name_bytes = name.encode('ascii', errors='ignore')
    header.extend(name_bytes)
    header.extend(b'\x00' * (28 - len(name_bytes)))  # pad with zeros
    
    # Determine the output file name if not specified
    if output_file is None:
        base_name = os.path.splitext(input_file)[0]
        output_file = f"{base_name}.xbc1"
    
    # Save the compressed file
    with open(output_file, 'wb') as f:
        f.write(header)
        f.write(compressed)
    
    compression_ratio = (1 - len(compressed) / len(data)) * 100
    
    print(f"Compression type: {compression_type}")
    print(f"Original size: {len(data)}")
    print(f"Size after compression: {len(compressed)}")
    print(f"Compression ratio: {compression_ratio:.1f}%")
    print(f"Name in header: {name}")
    print(f"Compressed to: {output_file}")

def read_arh_entries(arh_file: str) -> list:
    """
    Reads entries from an ARH file
    
    Returns:
        list of tuples: [(cache_id, size, uncompressed_size), ...]
    """
    entries = []
    with open(arh_file, 'rb') as f:
        # Read the header
        magic = f.read(4)
        if magic != b'arh2':
            raise ValueError(f"Invalid ARH file header: {magic}")
        
        num_entries = struct.unpack("<I", f.read(4))[0]
        entries_offset = struct.unpack("<I", f.read(4))[0]
        
        # Move to the entries
        f.seek(entries_offset)
        
        # Read all entries
        for _ in range(num_entries):
            cache_id = struct.unpack("<Q", f.read(8))[0]  # 8-byte cache_id
            size = struct.unpack("<I", f.read(4))[0]      # 4-byte size
            uncomp_size = struct.unpack("<I", f.read(4))[0]  # 4-byte uncompressed size
            entries.append((cache_id, size, uncomp_size))
    
    return entries

def calculate_padding(offset: int, alignment: int = 16) -> int:
    """Calculates the padding size for alignment"""
    return (alignment - (offset % alignment)) % alignment

def extract_ard_with_arh(ard_file: str, arh_file: str, output_dir: str = None, only_bdat: bool = False) -> None:
    """
    Extracts an ARD file using information from an ARH file
    
    Args:
        ard_file: path to the .ard file
        arh_file: path to the .arh file
        output_dir: directory to save the extracted files
        only_bdat: if True, only saves BDAT files
    """
    
    # Determine the output directory
    if output_dir is None:
        output_dir = os.path.splitext(ard_file)[0] + "_extracted"
    
    # Create the directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Read entries from the ARH file
    print("Reading ARH file...")
    entries = read_arh_entries(arh_file)
    print(f"Found {len(entries)} files")
    
    # Open the ARD file
    with open(ard_file, 'rb') as f:
        current_offset = 0
        saved_files = 0
        
        for index, (cache_id, size, uncomp_size) in enumerate(entries, 1):
            try:
                # Move to the start of the file, considering padding
                f.seek(current_offset)
                
                # Read the file data
                file_data = f.read(size)
                
                # Determine the file type by its header
                file_extension = ".dec"  # default extension
                is_bdat = False
                
                # Check if the file is XBC1
                is_xbc1 = file_data[:4] == b'xbc1'
                
                # Decompress if it's XBC1, regardless of uncomp_size
                if is_xbc1:
                    try:
                        decompressed = decompress_xbc1_file_data(file_data)
                        
                        # If uncomp_size is specified and doesn't match, print a warning
                        if uncomp_size > 0 and len(decompressed) != uncomp_size:
                            print(f"Warning: Decompressed file size mismatch {cache_id:016x}")
                            print(f"  Expected: {uncomp_size}, got: {len(decompressed)}")
                        
                        file_data = decompressed
                        
                        # Check the header of the decompressed data
                        if len(file_data) >= 4:
                            if file_data[:4] == b'BDAT':
                                file_extension = ".bdat"
                                is_bdat = True
                            # Other file types can be added as needed
                            
                    except Exception as e:
                        print(f"Error decompressing file {cache_id:016x}: {e}")
                        # Save the original data if decompression fails
                        file_extension = ".failed"
                elif uncomp_size > 0:
                    # If the file is marked as compressed but doesn't have an XBC1 header
                    print(f"Warning: File {cache_id:016x} is marked as compressed (uncomp_size={uncomp_size}), but is not XBC1")
                else:
                    # Check the header of uncompressed data
                    if len(file_data) >= 4:
                        if file_data[:4] == b'BDAT':
                            file_extension = ".bdat"
                            is_bdat = True
                        # Other file types can be added as needed
                
                # If only_bdat mode is enabled and it's not a BDAT file, skip saving
                if only_bdat and not is_bdat:
                    # Calculate the next offset, considering padding
                    current_offset += size + calculate_padding(size)
                    continue
                
                # Form the output file name with the correct extension
                output_file = os.path.join(output_dir, f"{cache_id:016x}{file_extension}")
                
                # Save the file
                with open(output_file, 'wb') as out_f:
                    out_f.write(file_data)
                
                saved_files += 1
                
                # Calculate the next offset, considering padding
                current_offset += size + calculate_padding(size)
                
                # Print progress
                if index % 100 == 0 or index == len(entries):
                    print(f"Processed files: {index}/{len(entries)}, saved: {saved_files}")
                
            except Exception as e:
                print(f"Error processing file {cache_id:016x}: {e}")
                # Calculate the next offset even if there's an error
                current_offset += size + calculate_padding(size)
                continue
        
        print("\nDone!")
        print(f"Processed files: {len(entries)}")
        print(f"Saved files: {saved_files}")

def decompress_xbc1_file_data(data: bytes) -> bytes:
    """
    Decompresses data in XBC1 format
    
    Args:
        data: compressed data with XBC1 header
    
    Returns:
        bytes: decompressed data
    """
    # Read the header
    compression_type = struct.unpack("<I", data[4:8])[0]
    uncompressed_size = struct.unpack("<I", data[8:12])[0]
    compressed_size = struct.unpack("<I", data[12:16])[0]
    
    # Get the compressed data after the header (48 bytes)
    compressed_data = data[48:48 + compressed_size]
    
    # Decompress
    if compression_type == CompType.ZLIB:
        decompressed = zlib.decompress(compressed_data)
    elif compression_type == CompType.ZSTD:
        dctx = zstd.ZstdDecompressor()
        decompressed = dctx.decompress(compressed_data)
    else:
        raise ValueError(f"Unknown compression type: {compression_type}")
    
    return decompressed

def create_ard_archive(input_dir: str, output_ard: str, output_arh: str, compress_files: bool = False) -> None:
    """
    Creates a new ARD archive from files in a directory (optimized version)
    
    Args:
        input_dir: directory with files to archive
        output_ard: path to create the ARD file
        output_arh: path to create the ARH file
        compress_files: if True, compress files in XBC1 format
    """
    # Get the list of all files in the directory
    print("Scanning directory...")
    files = []
    for root, _, filenames in os.walk(input_dir):
        for filename in filenames:
            file_path = os.path.join(root, filename)
            # Get the relative path
            rel_path = os.path.relpath(file_path, input_dir)
            files.append((file_path, rel_path))
    
    total_files = len(files)
    print(f"Found {total_files} files to archive")
    
    # Pre-process files (in parallel)
    processed_files = []
    
    def process_file(file_info):
        file_path, rel_path = file_info
        try:
            # Read the file
            with open(file_path, 'rb') as f:
                file_data = f.read()
            
            # Generate cache_id from the file name
            try:
                base_name = os.path.basename(file_path).split('.')[0]
                if base_name.isalnum() and len(base_name) <= 16:
                    cache_id = int(base_name, 16)
                else:
                    cache_id = hash(rel_path) & 0xFFFFFFFFFFFFFFFF
            except ValueError:
                cache_id = hash(rel_path) & 0xFFFFFFFFFFFFFFFF
            
            # Compress the file if needed
            uncompressed_size = 0
            if compress_files and file_data[:4] != b'xbc1':
                # Determine the compression type based on file size
                comp_type = CompType.ZSTD if len(file_data) > 1024*1024 else CompType.ZLIB
                
                # Compress the data
                if comp_type == CompType.ZLIB:
                    compressed = zlib.compress(file_data, level=9)
                else:
                    cctx = zstd.ZstdCompressor(level=22)
                    compressed = cctx.compress(file_data)
                
                # Create the XBC1 header
                header = bytearray()
                header.extend(b'xbc1')
                header.extend(struct.pack("<I", comp_type))
                header.extend(struct.pack("<I", len(file_data)))
                header.extend(struct.pack("<I", len(compressed)))
                
                # Calculate the hash
                simple_hash = sum(file_data) & 0xFFFFFFFF
                header.extend(struct.pack("<I", simple_hash))
                
                # Add the file name
                name = os.path.basename(rel_path)
                if len(name) > 27:
                    name = name[:27]
                name_bytes = name.encode('ascii', errors='ignore')
                header.extend(name_bytes)
                header.extend(b'\x00' * (28 - len(name_bytes)))
                
                # Combine the header and compressed data
                file_data = header + compressed
                uncompressed_size = len(file_data)
            
            return (cache_id, file_data, uncompressed_size)
        except Exception as e:
            print(f"Error processing file {rel_path}: {e}")
            return None
    
    # Use ThreadPoolExecutor for parallel file processing
    print("Processing files...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        # Use tqdm to show progress
        for result in tqdm(executor.map(process_file, files), total=total_files, desc="Processing files"):
            if result:
                processed_files.append(result)
    
    # Sort files by cache_id for more efficient searching
    processed_files.sort(key=lambda x: x[0])
    
    # Create the ARD file
    print("Creating ARD file...")
    entries = []
    current_offset = 0
    
    with open(output_ard, 'wb') as ard_file:
        for cache_id, file_data, uncompressed_size in tqdm(processed_files, desc="Writing ARD"):
            # Write the file to ARD
            ard_file.write(file_data)
            
            # Calculate padding
            padding_size = calculate_padding(len(file_data))
            if padding_size > 0:
                ard_file.write(b'\x00' * padding_size)
            
            # Save information for ARH
            entries.append((cache_id, len(file_data), uncompressed_size))
            
            # Update the offset
            current_offset += len(file_data) + padding_size
    
    # Create the ARH file
    print("Creating ARH file...")
    with open(output_arh, 'wb') as arh_file:
        # Write the header
        arh_file.write(b'arh2')  # Magic number
        arh_file.write(struct.pack("<I", len(entries)))  # Number of entries
        
        # Offset to entries (after the header)
        entries_offset = 16  # 4 (magic) + 4 (num_entries) + 4 (entries_offset) + 4 (padding)
        entries_zero = 0
        arh_file.write(struct.pack("<I", entries_offset))
        arh_file.write(struct.pack("<I", entries_zero))
        
        # Write the entries
        for cache_id, size, uncomp_size in tqdm(entries, desc="Writing ARH"):
            arh_file.write(struct.pack("<Q", cache_id))  # 8-byte cache_id
            arh_file.write(struct.pack("<I", size))      # 4-byte size
            arh_file.write(struct.pack("<I", uncomp_size))  # 4-byte uncompressed size
    
    print("\nDone!")
    print(f"Created ARD archive: {output_ard}")
    print(f"Created ARH file: {output_arh}")
    print(f"Added files: {len(entries)} out of {total_files}")

# Modify the main code to support packing
if __name__ == "__main__":
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(description='XBC1 file archiver/extractor')
    parser.add_argument('input_file', help='Input file or directory')
    parser.add_argument('arh_file', nargs='?', help='ARH file (required for ARD mode)')
    parser.add_argument('output_dir', nargs='?', help='Output directory or file (optional)')
    parser.add_argument('-c', '--compress', action='store_true', help='Compression mode (default is decompression)')
    parser.add_argument('-t', '--type', type=int, choices=[1, 3], default=1, 
                        help='Compression type (1=ZLIB, 3=ZSTD) for compression mode')
    parser.add_argument('-n', '--name', help='File name for XBC1 header (optional)')
    parser.add_argument('--ard', action='store_true', help='ARD extraction mode (requires ARH file)')
    parser.add_argument('--only-bdat', action='store_true', help='Only save BDAT files')
    parser.add_argument('--create-ard', action='store_true', help='Create a new ARD archive from a directory')
    parser.add_argument('--compress-files', action='store_true', help='Compress files when creating ARD archive')
    
    args = parser.parse_args()
    
    try:
        if args.create_ard:
            if not args.arh_file or not args.output_dir:
                print("Error: to create an ARD archive, output ARD and ARH files must be specified")
                print("Example: python script.py --create-ard input_dir output.ard output.arh")
                sys.exit(1)
            create_ard_archive(args.input_file, args.arh_file, args.output_dir, args.compress_files)
        elif args.ard:
            if not args.arh_file:
                print("Error: to extract ARD, an ARH file must be specified")
                sys.exit(1)
            extract_ard_with_arh(args.input_file, args.arh_file, args.output_dir, args.only_bdat)
        elif args.compress:
            compress_xbc1_file(args.input_file, args.output_dir, CompType(args.type), args.name)
        else:
            decompress_xbc1_file(args.input_file, args.output_dir)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
