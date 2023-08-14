from configparser import ConfigParser

#Get the configparser object
config_object = ConfigParser()


config_object["FILEINFO"] = {
    "Input File": "product_data.csv",
    "Output File": "product_data.parquet",
    "Command": "python testing.py -i product_data.csv -o product_data.parquet"
}


#Write the above sections to config.ini file
with open('config.ini', 'w') as conf:
    config_object.write(conf)