import configparser
from pathlib import Path


def main():
    config = configparser.ConfigParser()
    config.read_file(open(f"{Path(__file__).parents[0]}/dwh.cfg"))
    #config.read_file(open('dwh.cfg'))
    s3=config.get('S3','LOG_DATA')
    role=config.get('IAM_ROLE','POLICY_ARN')
    jsonpath = config.get('S3','LOG_JSONPATH')
    print(s3,role,jsonpath)
    print(config['S3']['LOG_DATA'])


if __name__=="__main__":
    main()
