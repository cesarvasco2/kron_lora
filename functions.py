import struct
import json
# converte o hexadecimal em float point
def convert(hex):
    try:
        conversion = round(struct.unpack('!f', bytes.fromhex(hex+'00'))[0], 2)
        return conversion
    except:
        pass    
# cria uma lista que separa o payload em grupos de 4 bytes
def hex_split(payload_split):
    try:
        bytes_break = 8
        result =[payload_split[y-bytes_break:y] for y in range(bytes_break, len(payload_split)+bytes_break,bytes_break)]
        return result
    except:
        pass
# cria um dict a partir do lora payload 
def payload_converter(lora_payload):
    try:
        payload_dict = {}
        for l in hex_split(lora_payload):
            key = str(l[:2])
            value = convert(str(l[2:9]))
            payload_dict[key] = value
        return json.dumps(payload_dict)
    except:
        pass    

'''payload_lora = payload_converter('01000000020000002a3f80002b0000002c407fe62d000000')    
print(payload_lora)'''
