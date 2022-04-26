import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tracks", type=str, required=True)
    # parser.add_argument("--triplets", type=str, required=True)
    args = parser.parse_args()

    tracks_path = args.tracks
    # triplets_path = args.triplets

    with open(tracks_path, 'r', encoding='latin-1') as f:
        for line in f:
            print(line)
            line = line.strip()
            line = line.split('<SEP>')
            print(line)


if __name__ == '__main__':
    main()
