import etcd3
import argparse
import json
import urllib.parse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--etcd', help='hostname of etcd server', required=True)
    parser.add_argument('--bucket-name', required=True)
    parser.add_argument('--strings-key', required=True)
    parser.add_argument('--newline-index', required=True)
    parser.add_argument('--template-file', required=True)
    parser.add_argument('--line-count', type=int, required=True)
    parser.add_argument('--lines-per-task', type=int, required=True)
    parser.add_argument('--output-key', required=True)

    args = parser.parse_args()

    client = etcd3.client(host=args.etcd)

    encoded_output_key = urllib.parse.quote(args.output_key)

    with open(args.template_file, 'r') as template_file:
        template = template_file.read()

    task_count = int((args.line_count + args.lines_per_task - 1 ) / args.lines_per_task)

    for task_index in range(0,task_count):
        start_line = task_index * args.lines_per_task;
        end_line = (task_index + 1) * args.lines_per_task - 1;
        if end_line >= args.line_count:
            end_line = args.line_count - 1

        output_key = args.output_key + f'/{task_index}.vecs'

        task_init = {
            'bucket_name': args.bucket_name,
            'strings_key': args.strings_key,
            'newline_index': args.newline_index,
            'output_key': output_key,
            'start_line': start_line,
            'end_line': end_line,
            'template': template
        }

        task = {
            'status': 'pending',
            'init': task_init
        }

        task_str = json.dumps(task)
        task_key = f'/services/tasks/vectorizer/mxbai/{args.bucket_name}/{encoded_output_key}/{task_index}'

        print(task_key)

        client.put(task_key, task_str)


    pass

if __name__ == "__main__":
    main()
