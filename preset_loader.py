import pandas as pd
import os


def load_presets(loc="config_files"):
    
    def sensor_config(conv_mul, unit, description, measured=0, offset=0):
    
        config = {
                "read_to_unit":conv_mul,
                "unit":unit,
                "description":description,
                "measured":measured,
                "offset":offset,
                }

        return config

    
    config_list = []
    
    for file in os.listdir(loc):
        temp_config_file = pd.read_excel(os.path.join(loc, file))
        temp_config_file["1V odpowiada X jednostkom"].fillna(1, inplace=True)
        temp_config_file.fillna("", inplace=True)

        template={
            "nazwa":file.split("_")[0],
            "id":file.split("_")[1][:-5],
        }

        temp_cfg = {}

        for row in temp_config_file.iterrows():
            temp_cfg[row[1]['wejscie']] = sensor_config(row[1]['1V odpowiada X jednostkom'],
                                                        row[1]['jednostka'],
                                                        row[1]['opis'],
                                                        measured=row[1]['wielkosc_mierzona'])

        template["config"] = temp_cfg
        
        config_list.append(template)
    
    return config_list