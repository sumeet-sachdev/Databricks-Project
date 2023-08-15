from configparser import ConfigParser

#Get the configparser object
config_object = ConfigParser()


config_object["InputFile"] = {
    "Input File": "product_data.csv",

}
config_object["OutputFile"] = {
    "Output File": "product_data.parquet",

}


#Write the above sections to config.ini file
with open('config.ini', 'w') as conf:
    config_object.write(conf)