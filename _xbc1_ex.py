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
    Распаковывает XBC1 файл
    
    Args:
        input_file: путь к файлу для распаковки
        output_file: путь для сохранения распакованного файла (если None, создается автоматически)
    """
    
    # Читаем весь файл
    with open(input_file, 'rb') as f:
        data = f.read()
    
    # Проверяем магическое число
    if data[:4] != b'xbc1':
        raise ValueError(f"Не XBC1 файл! Магическое число: {data[:4]}")
    
    # Читаем заголовок
    compression_type = struct.unpack("<I", data[4:8])[0]
    uncompressed_size = struct.unpack("<I", data[8:12])[0]
    compressed_size = struct.unpack("<I", data[12:16])[0]
    # hash = struct.unpack("<I", data[16:20])[0]  # не используется
    name = data[20:48].split(b'\x00')[0].decode('ascii', errors='ignore')
    
    print(f"Тип сжатия: {compression_type}")
    print(f"Размер до распаковки: {compressed_size}")
    print(f"Размер после распаковки: {uncompressed_size}")
    print(f"Имя файла в заголовке: {name}")
    
    # Получаем сжатые данные после заголовка (48 байт)
    compressed_data = data[48:48 + compressed_size]
    
    # Распаковываем
    if compression_type == CompType.ZLIB:
        try:
            decompressed = zlib.decompress(compressed_data)
        except zlib.error as e:
            raise ValueError(f"Ошибка распаковки ZLIB: {e}")
    
    elif compression_type == CompType.ZSTD:
        try:
            dctx = zstd.ZstdDecompressor()
            decompressed = dctx.decompress(compressed_data)
        except zstd.ZstdError as e:
            raise ValueError(f"Ошибка распаковки ZSTD: {e}")
    
    else:
        raise ValueError(f"Неизвестный тип сжатия: {compression_type}")
    
    # Проверяем размер
    if len(decompressed) != uncompressed_size:
        raise ValueError(f"Неправильный размер распакованных данных: ожидалось {uncompressed_size}, получено {len(decompressed)}")
    
    # Определяем имя выходного файла если не указано
    if output_file is None:
        base_name = os.path.splitext(input_file)[0]
        output_file = f"{base_name}.dec"
    
    # Сохраняем распакованные данные
    with open(output_file, 'wb') as f:
        f.write(decompressed)
    
    print(f"Распаковано в: {output_file}")

def compress_xbc1_file(input_file: str, output_file: str = None, compression_type: CompType = CompType.ZLIB, name: str = "") -> None:
    """
    Запаковывает файл в формат XBC1
    
    Args:
        input_file: путь к файлу для запаковки
        output_file: путь для сохранения запакованного файла (если None, создается автоматически)
        compression_type: тип сжатия (ZLIB или ZSTD)
        name: имя файла для записи в заголовок (максимум 27 символов)
    """
    
    # Читаем исходный файл
    with open(input_file, 'rb') as f:
        data = f.read()
    
    # Сжимаем данные
    if compression_type == CompType.ZLIB:
        try:
            compressed = zlib.compress(data, level=9)  # максимальное сжатие
        except zlib.error as e:
            raise ValueError(f"Ошибка сжатия ZLIB: {e}")
    
    elif compression_type == CompType.ZSTD:
        try:
            cctx = zstd.ZstdCompressor(level=22)  # максимальное сжатие
            compressed = cctx.compress(data)
        except zstd.ZstdError as e:
            raise ValueError(f"Ошибка сжатия ZSTD: {e}")
    
    else:
        raise ValueError(f"Неизвестный тип сжатия: {compression_type}")

    # Если имя не указано, используем имя входного файла
    if not name:
        name = os.path.basename(input_file)
    
    # Обрезаем имя если оно слишком длинное (оставляем место для нуль-терминатора)
    if len(name) > 27:
        name = name[:27]
    
    # Создаем заголовок
    header = bytearray()
    header.extend(b'xbc1')  # магическое число
    header.extend(struct.pack("<I", compression_type))  # тип сжатия
    header.extend(struct.pack("<I", len(data)))  # несжатый размер
    header.extend(struct.pack("<I", len(compressed)))  # сжатый размер
    
    # Вычисляем простой хеш (можно модифицировать если нужен конкретный алгоритм)
    simple_hash = sum(data) & 0xFFFFFFFF
    header.extend(struct.pack("<I", simple_hash))  # хеш
    
    # Добавляем имя файла (28 байт включая нуль-терминатор)
    name_bytes = name.encode('ascii', errors='ignore')
    header.extend(name_bytes)
    header.extend(b'\x00' * (28 - len(name_bytes)))  # дополняем нулями
    
    # Определяем имя выходного файла если не указано
    if output_file is None:
        base_name = os.path.splitext(input_file)[0]
        output_file = f"{base_name}.xbc1"
    
    # Сохраняем запакованный файл
    with open(output_file, 'wb') as f:
        f.write(header)
        f.write(compressed)
    
    compression_ratio = (1 - len(compressed) / len(data)) * 100
    
    print(f"Тип сжатия: {compression_type}")
    print(f"Исходный размер: {len(data)}")
    print(f"Размер после сжатия: {len(compressed)}")
    print(f"Степень сжатия: {compression_ratio:.1f}%")
    print(f"Имя в заголовке: {name}")
    print(f"Запаковано в: {output_file}")

def read_arh_entries(arh_file: str) -> list:
    """
    Читает записи из ARH файла
    
    Returns:
        list of tuples: [(cache_id, size, uncompressed_size), ...]
    """
    entries = []
    with open(arh_file, 'rb') as f:
        # Читаем заголовок
        magic = f.read(4)
        if magic != b'arh2':
            raise ValueError(f"Неверный заголовок ARH файла: {magic}")
        
        num_entries = struct.unpack("<I", f.read(4))[0]
        entries_offset = struct.unpack("<I", f.read(4))[0]
        
        # Переходим к записям
        f.seek(entries_offset)
        
        # Читаем все записи
        for _ in range(num_entries):
            cache_id = struct.unpack("<Q", f.read(8))[0]  # 8 байт cache_id
            size = struct.unpack("<I", f.read(4))[0]      # 4 байта размер
            uncomp_size = struct.unpack("<I", f.read(4))[0]  # 4 байта размер распакованного
            entries.append((cache_id, size, uncomp_size))
    
    return entries

def calculate_padding(offset: int, alignment: int = 16) -> int:
    """Вычисляет размер паддинга для выравнивания"""
    return (alignment - (offset % alignment)) % alignment

def extract_ard_with_arh(ard_file: str, arh_file: str, output_dir: str = None, only_bdat: bool = False) -> None:
    """
    Распаковывает ARD файл используя информацию из ARH
    
    Args:
        ard_file: путь к .ard файлу
        arh_file: путь к .arh файлу
        output_dir: папка для распакованных файлов
        only_bdat: если True, сохраняет только BDAT файлы
    """
    
    # Определяем выходную директорию
    if output_dir is None:
        output_dir = os.path.splitext(ard_file)[0] + "_extracted"
    
    # Создаем директорию если её нет
    os.makedirs(output_dir, exist_ok=True)
    
    # Читаем записи из ARH файла
    print("Чтение ARH файла...")
    entries = read_arh_entries(arh_file)
    print(f"Найдено {len(entries)} файлов")
    
    # Открываем ARD файл
    with open(ard_file, 'rb') as f:
        current_offset = 0
        saved_files = 0
        
        for index, (cache_id, size, uncomp_size) in enumerate(entries, 1):
            try:
                # Переходим к началу файла с учетом паддинга
                f.seek(current_offset)
                
                # Читаем данные файла
                file_data = f.read(size)
                
                # Определяем тип файла по заголовку
                file_extension = ".dec"  # расширение по умолчанию
                is_bdat = False
                
                # Проверяем, является ли файл XBC1
                is_xbc1 = file_data[:4] == b'xbc1'
                
                # Распаковываем если это XBC1, независимо от uncomp_size
                if is_xbc1:
                    try:
                        decompressed = decompress_xbc1_file_data(file_data)
                        
                        # Если uncomp_size указан и не совпадает, выводим предупреждение
                        if uncomp_size > 0 and len(decompressed) != uncomp_size:
                            print(f"Предупреждение: Несовпадение размера распакованного файла {cache_id:016x}")
                            print(f"  Ожидалось: {uncomp_size}, получено: {len(decompressed)}")
                        
                        file_data = decompressed
                        
                        # Проверяем заголовок распакованных данных
                        if len(file_data) >= 4:
                            if file_data[:4] == b'BDAT':
                                file_extension = ".bdat"
                                is_bdat = True
                            # Можно добавить другие типы файлов по мере необходимости
                            
                    except Exception as e:
                        print(f"Ошибка при распаковке файла {cache_id:016x}: {e}")
                        # Сохраняем оригинальные данные если распаковка не удалась
                        file_extension = ".failed"
                elif uncomp_size > 0:
                    # Если файл помечен как сжатый, но не имеет XBC1 заголовка
                    print(f"Предупреждение: Файл {cache_id:016x} помечен как сжатый (uncomp_size={uncomp_size}), но не является XBC1")
                else:
                    # Проверяем заголовок несжатых данных
                    if len(file_data) >= 4:
                        if file_data[:4] == b'BDAT':
                            file_extension = ".bdat"
                            is_bdat = True
                        # Можно добавить другие типы файлов по мере необходимости
                
                # Если включен режим only_bdat и это не BDAT файл, пропускаем сохранение
                if only_bdat and not is_bdat:
                    # Вычисляем следующее смещение с учетом паддинга
                    current_offset += size + calculate_padding(size)
                    continue
                
                # Формируем имя выходного файла с правильным расширением
                output_file = os.path.join(output_dir, f"{cache_id:016x}{file_extension}")
                
                # Сохраняем файл
                with open(output_file, 'wb') as out_f:
                    out_f.write(file_data)
                
                saved_files += 1
                
                # Вычисляем следующее смещение с учетом паддинга
                current_offset += size + calculate_padding(size)
                
                # Выводим прогресс
                if index % 100 == 0 or index == len(entries):
                    print(f"Обработано файлов: {index}/{len(entries)}, сохранено: {saved_files}")
                
            except Exception as e:
                print(f"Ошибка при обработке файла {cache_id:016x}: {e}")
                # Вычисляем следующее смещение даже при ошибке
                current_offset += size + calculate_padding(size)
                continue
        
        print("\nГотово!")
        print(f"Обработано файлов: {len(entries)}")
        print(f"Сохранено файлов: {saved_files}")

def decompress_xbc1_file_data(data: bytes) -> bytes:
    """
    Распаковывает данные в формате XBC1
    
    Args:
        data: сжатые данные с XBC1 заголовком
    
    Returns:
        bytes: распакованные данные
    """
    # Читаем заголовок
    compression_type = struct.unpack("<I", data[4:8])[0]
    uncompressed_size = struct.unpack("<I", data[8:12])[0]
    compressed_size = struct.unpack("<I", data[12:16])[0]
    
    # Получаем сжатые данные после заголовка (48 байт)
    compressed_data = data[48:48 + compressed_size]
    
    # Распаковываем
    if compression_type == CompType.ZLIB:
        decompressed = zlib.decompress(compressed_data)
    elif compression_type == CompType.ZSTD:
        dctx = zstd.ZstdDecompressor()
        decompressed = dctx.decompress(compressed_data)
    else:
        raise ValueError(f"Неизвестный тип сжатия: {compression_type}")
    
    return decompressed

def create_ard_archive(input_dir: str, output_ard: str, output_arh: str, compress_files: bool = False) -> None:
    """
    Создает новый ARD архив из файлов в директории (оптимизированная версия)
    
    Args:
        input_dir: директория с файлами для архивации
        output_ard: путь для создания ARD файла
        output_arh: путь для создания ARH файла
        compress_files: если True, сжимать файлы в формате XBC1
    """
    # Получаем список всех файлов в директории
    print("Сканирование директории...")
    files = []
    for root, _, filenames in os.walk(input_dir):
        for filename in filenames:
            file_path = os.path.join(root, filename)
            # Получаем относительный путь
            rel_path = os.path.relpath(file_path, input_dir)
            files.append((file_path, rel_path))
    
    total_files = len(files)
    print(f"Найдено {total_files} файлов для архивации")
    
    # Предварительная обработка файлов (параллельно)
    processed_files = []
    
    def process_file(file_info):
        file_path, rel_path = file_info
        try:
            # Читаем файл
            with open(file_path, 'rb') as f:
                file_data = f.read()
            
            # Генерируем cache_id из имени файла
            try:
                base_name = os.path.basename(file_path).split('.')[0]
                if base_name.isalnum() and len(base_name) <= 16:
                    cache_id = int(base_name, 16)
                else:
                    cache_id = hash(rel_path) & 0xFFFFFFFFFFFFFFFF
            except ValueError:
                cache_id = hash(rel_path) & 0xFFFFFFFFFFFFFFFF
            
            # Сжимаем файл если нужно
            uncompressed_size = 0
            if compress_files and file_data[:4] != b'xbc1':
                # Определяем тип сжатия по размеру файла
                comp_type = CompType.ZSTD if len(file_data) > 1024*1024 else CompType.ZLIB
                
                # Сжимаем данные
                if comp_type == CompType.ZLIB:
                    compressed = zlib.compress(file_data, level=9)
                else:
                    cctx = zstd.ZstdCompressor(level=22)
                    compressed = cctx.compress(file_data)
                
                # Создаем заголовок XBC1
                header = bytearray()
                header.extend(b'xbc1')
                header.extend(struct.pack("<I", comp_type))
                header.extend(struct.pack("<I", len(file_data)))
                header.extend(struct.pack("<I", len(compressed)))
                
                # Вычисляем хеш
                simple_hash = sum(file_data) & 0xFFFFFFFF
                header.extend(struct.pack("<I", simple_hash))
                
                # Добавляем имя файла
                name = os.path.basename(rel_path)
                if len(name) > 27:
                    name = name[:27]
                name_bytes = name.encode('ascii', errors='ignore')
                header.extend(name_bytes)
                header.extend(b'\x00' * (28 - len(name_bytes)))
                
                # Объединяем заголовок и сжатые данные
                file_data = header + compressed
                uncompressed_size = len(file_data)
            
            return (cache_id, file_data, uncompressed_size)
        except Exception as e:
            print(f"Ошибка при обработке файла {rel_path}: {e}")
            return None
    
    # Используем ThreadPoolExecutor для параллельной обработки файлов
    print("Обработка файлов...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        # Используем tqdm для отображения прогресса
        for result in tqdm(executor.map(process_file, files), total=total_files, desc="Обработка файлов"):
            if result:
                processed_files.append(result)
    
    # Сортируем файлы по cache_id для более эффективного поиска
    processed_files.sort(key=lambda x: x[0])
    
    # Создаем ARD файл
    print("Создание ARD файла...")
    entries = []
    current_offset = 0
    
    with open(output_ard, 'wb') as ard_file:
        for cache_id, file_data, uncompressed_size in tqdm(processed_files, desc="Запись ARD"):
            # Записываем файл в ARD
            ard_file.write(file_data)
            
            # Вычисляем паддинг
            padding_size = calculate_padding(len(file_data))
            if padding_size > 0:
                ard_file.write(b'\x00' * padding_size)
            
            # Сохраняем информацию для ARH
            entries.append((cache_id, len(file_data), uncompressed_size))
            
            # Обновляем смещение
            current_offset += len(file_data) + padding_size
    
    # Создаем ARH файл
    print("Создание ARH файла...")
    with open(output_arh, 'wb') as arh_file:
        # Записываем заголовок
        arh_file.write(b'arh2')  # Магическое число
        arh_file.write(struct.pack("<I", len(entries)))  # Количество записей
        
        # Смещение до записей (после заголовка)
        entries_offset = 16  # 4 (magic) + 4 (num_entries) + 4 (entries_offset) + 4 (padding)
        entries_zero = 0
        arh_file.write(struct.pack("<I", entries_offset))
        arh_file.write(struct.pack("<I", entries_zero))
        
        # Записываем записи
        for cache_id, size, uncomp_size in tqdm(entries, desc="Запись ARH"):
            arh_file.write(struct.pack("<Q", cache_id))  # 8 байт cache_id
            arh_file.write(struct.pack("<I", size))      # 4 байта размер
            arh_file.write(struct.pack("<I", uncomp_size))  # 4 байта размер распакованного
    
    print("\nГотово!")
    print(f"Создан ARD архив: {output_ard}")
    print(f"Создан ARH файл: {output_arh}")
    print(f"Добавлено файлов: {len(entries)} из {total_files}")

# Модифицируем основной код для поддержки упаковки
if __name__ == "__main__":
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(description='XBC1 файловый архиватор/распаковщик')
    parser.add_argument('input_file', help='Входной файл или директория')
    parser.add_argument('arh_file', nargs='?', help='ARH файл (необходим для режима ARD)')
    parser.add_argument('output_dir', nargs='?', help='Выходная директория или файл (необязательно)')
    parser.add_argument('-c', '--compress', action='store_true', help='Режим упаковки (по умолчанию распаковка)')
    parser.add_argument('-t', '--type', type=int, choices=[1, 3], default=1, 
                        help='Тип сжатия (1=ZLIB, 3=ZSTD) для режима упаковки')
    parser.add_argument('-n', '--name', help='Имя файла для заголовка XBC1 (необязательно)')
    parser.add_argument('--ard', action='store_true', help='Режим извлечения ARD архива (требует ARH файл)')
    parser.add_argument('--only-bdat', action='store_true', help='Сохранять только BDAT файлы')
    parser.add_argument('--create-ard', action='store_true', help='Создать новый ARD архив из директории')
    parser.add_argument('--compress-files', action='store_true', help='Сжимать файлы при создании ARD архива')
    
    args = parser.parse_args()
    
    try:
        if args.create_ard:
            if not args.arh_file or not args.output_dir:
                print("Ошибка: для создания ARD архива необходимо указать выходные ARD и ARH файлы")
                print("Пример: python script.py --create-ard input_dir output.ard output.arh")
                sys.exit(1)
            create_ard_archive(args.input_file, args.arh_file, args.output_dir, args.compress_files)
        elif args.ard:
            if not args.arh_file:
                print("Ошибка: для распаковки ARD необходимо указать ARH файл")
                sys.exit(1)
            extract_ard_with_arh(args.input_file, args.arh_file, args.output_dir, args.only_bdat)
        elif args.compress:
            compress_xbc1_file(args.input_file, args.output_dir, CompType(args.type), args.name)
        else:
            decompress_xbc1_file(args.input_file, args.output_dir)
    except Exception as e:
        print(f"Ошибка: {e}")
        sys.exit(1)
