"""
Targeted generation of FBA
"""
import flowsa

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', help='FBA source name')
    parser.add_argument('--year', help='FBA year')
    args = vars(parser.parse_args())

    flowsa.generateflowbyactivity.main(
        source=args['source'], year=args['year'])
