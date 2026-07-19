import pandas as pd
import argparse


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean data by deleting rows that have empty fields")
    parser.add_argument("input", help="path to target .csv file")
    parser.add_argument("-o", "--output", dest="output", help="output image path (if omitted adds suffix _clean)")
    args = parser.parse_args()

    df = pd.read_csv(args.input, sep=';')
    output_path = args.output if args.output else args.input.replace('.csv', '_clean.csv')
    df = df.dropna(subset=['P1','P4','P2','P0','N10','N4','N25','N1','N05','TS'])
    df.to_csv(output_path, index=False)
