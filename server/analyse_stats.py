import pstats
import os
import json


def load(uuid):
    stats = pstats.Stats(os.path.join('pstats',uuid))
    stats.calc_callees()
    stats.sort_stats('cum')
    return stats

def keys_to_str(dictionary):
    for key in dictionary.keys():
        str_key = to_str(key)
        dictionary[str_key] = dictionary.pop(key)
        if type(dictionary[str_key]) == dict:
            keys_to_str(dictionary[str_key])
        elif len(dictionary[str_key])>4:
            keys_to_str(dictionary[str_key][4])
    return dictionary

def to_str(tup):
    if tup[0]=='~' and tup[1]==0:
        return tup[2]
    else:
        return '{0}::{1}::{2}'.format(tup[0],tup[1],tup[2])


def write_json(dictionary, uuid):
    with open(os.path.join('pstats',uuid+'.json'),'w') as f:
        f.write(json.dumps(dictionary))



if __name__ == '__main__':

    for f in os.listdir('pstats'):
        if '.' in f:
            continue
        stats = load(f)
        callees = keys_to_str(stats.all_callees)
        total_tt = stats.total_tt
        stats = keys_to_str(stats.stats)
        write_json({'stats':stats,'callees':callees,'total_tt':total_tt}, f)
        exit(0)
