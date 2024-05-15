import boto3
import argparse
import struct
import sys

s3 = boto3.client('s3')

def byte_offset_for_line_number(bucket_name, index_key, line_number):
    offset_in_index = line_number * 8
    r = f'bytes={offset_in_index}-{offset_in_index+7}'
    print(f'range in byte index: {r}', file=sys.stderr)

    response=s3.get_object(
        Bucket=bucket_name,
        Key=index_key,
        Range=r
    )
    data = response['Body'].read()
    return struct.unpack('<q', data)[0]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket-name', required=True)
    parser.add_argument('--strings-key', required=True)
    parser.add_argument('--newline-index', required=True)
    parser.add_argument('line', type=int)

    args = parser.parse_args()

    start_byte = byte_offset_for_line_number(args.bucket_name, args.newline_index, args.line)
    end_byte = byte_offset_for_line_number(args.bucket_name, args.newline_index, args.line + 1) - 1

    obj = s3.get_object(Bucket=bucket_name, Key=strings_key, Range=f'bytes={start_byte}-{end_byte}')
    print(obj['Body'].read())

if __name__ == "__main__":
    main()
