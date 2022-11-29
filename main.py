import sys
import json
import paho.mqtt.client as mqtt
import boto3
import time
from datetime import date, datetime
import calendar
import functions as f
sqs = boto3.resource('sqs', region_name='us-east-1')
queue = sqs.get_queue_by_name(QueueName='processador_entrada')
#configurações do broker:
Broker = 'servermqtt.duckdns.org'
PortaBroker = 1883
Usuario = 'afira'
Senha = 'afira'
KeepAliveBroker = 60
TopicoSubscribe = 'GIOT-GW/#' #Topico que ira se inscrever
#Callback - conexao ao broker realizada
def on_connect(client, userdata, flags, rc):
    print('[STATUS] Conectado ao Broker. Resultado de conexao: {}'.format(str(rc)))
#faz subscribe automatico no topico
    client.subscribe(TopicoSubscribe)
#Callback - mensagem recebida do broker
def on_message(client, userdata, msg):
    mensagem_recebida = str(msg.payload.decode('utf-8') )
    #print("[MESAGEM RECEBIDA] Topico: "+msg.topic+" / Mensagem: "+mensagem_recebida)
    list_payload = json.loads(mensagem_recebida)
    dict_payload = list_payload[0]
    dict_data = json.loads(f.payload_converter(dict_payload['data']))
    print(dict_data)
    lista_de_campos = [
        {'key':'tensao_trifasica','type':'str','fields':'00'},
        {'key':'tensao','type':'str','fields':('01','02','03')},
        {'key':'tensao_fase_neutro','type':'str','fields':('04','05','06')},
        {'key':'corrente_trifasica','type':'str','fields':'07'},
        {'key':'corrente_neutro','type':'str','fields':'08'},
        {'key':'corrente','type':'str','fields':('09','0A','0B')},
        {'key':'potencia_ativa_trifasica','type':'str','fields':'10'},
        {'key':'potencia_ativa_linha','type':'str','fields':('11','12','13')},
        {'key':'potencia_reativa_trifasica','type':'str','fields':'14'},
        {'key':'potencia_reativa_linha','type':'str','fields':('15','16','17')},
        {'key':'potencia_aparente_trifasica','type':'str','fields':'18'},
        {'key':'potencia_aparente_linha','type':'str','fields':('19','1A','1B')},
        {'key':'fator_potencia_trifasica','type':'str','fields':'1C'},
        {'key':'fator_potencia_linha','type':'str','fields':('1D','1E','1F')},
        {'key':'pulso','type':'pulso','fields':('24','25','26')},
        {'key':'entradas_4a20','type':'4a20','fields':('2C','2D')},
        {'key':'frequencia','type':'str','fields':('0C','0D','0E')},
        {'key':'frequencia_iec_10s','type':'str','fields':'0F'},
        {'key':'energia_ativa','type':'int','fields':'2E'},
        {'key':'energia_ativa_negativa','type':'int','fields':'30'},
        {'key':'energia_reativa','type':'str','fields':'2F'},
        {'key':'energia_reativa_negativa','type':'str','fields':'31'},
        {'key':'maxima_demanda_ativa','type':'str','fields':'32'},
        {'key':'demanda_ativa','type':'str','fields':'33'},
        {'key':'maxima_demanda_aparente','type':'str','fields':'34'},
        {'key':'demanda_aparente','type':'str','fields':'35'},
        {'key':'temperatura','type':'str','fields':'47'},
        {'key':'thd_tensao_fase','type':'str','fields':('3B','3C','3D')},
        {'key':'thd_corrente_fase','type':'str','fields':('3E','3F','40')},
        {'key':'ct_pulso','type':'int','fields':('24','25','26')},
        {'key':'status','type':'pulso','fields':('27','28','29')},
        {'key':'rele','type':'int','fields':('2A','2B')},
        {'key':'horimetro','type':'str','fields':('58',)},
        {'key':'status_carga','type':'int','fields':'57'},
        {'key':'codigo_erro','type':'int','fields':'FF'},
    ]
    dia_semana = date.today() 
    data_e_hora_atuais = datetime.now() 
    dict_save = {}
    dict_save['id_dispositivo'] = dict_payload['devEUI']
    dict_save['data_hora_dispositivo'] = data_e_hora_atuais.strftime('%Y-%m-%d %H:%M:%S')#dict_payload['time'][0:].replace('T', ' ')
    for campo in lista_de_campos:
        if isinstance(campo['fields'],tuple):
            elementos_do_campo = []
            if campo['type']=='4a20':
                for field in campo['fields']:
                    if field in dict_data:
                        print ('entrei aqui')
                        elementos_do_campo.append(int(float(dict_data[field])*100))
            elif campo['type']=='array':
                for field in campo['fields']:
                    if field in dict_payload['metadata']:
                        elementos_do_campo.append(str(dict_data[field]))
            elif campo['type']=='pulso':
                for field in campo['fields']:
                    if not field in dict_data:
                        elementos_do_campo.append(0)
                    else:
                        elementos_do_campo.append(int(dict_data[field]))                            
            else:
                for field in campo['fields']:
                    if field in dict_data:
                        if campo['type'] == 'int':
                            elementos_do_campo.append(int(dict_data[field]))   
                        else:
                            elementos_do_campo.append(str(dict_data[field]))   
            dict_save[campo['key']] = elementos_do_campo
        else:
            if campo['type']=='int':
                if campo['fields'] in dict_data:
                    dict_save[campo['key']] = int(dict_data[campo['fields']])
            else:
                if campo['fields'] in dict_data:
                    dict_save[campo['key']] = str(dict_data[campo['fields']])
    dict_save['codigo_produto'] = 24
    dict_save['timestamp_servidor'] = int(datetime.now().timestamp())
    #format_date = dict_payload['time'].strftime('%Y-%m-%d %H:%M:%S')
    dict_save['timestamp_dispositivo'] = int(datetime.now().timestamp())#int(format_date.timestamp())
    dict_save['dia_sem'] = calendar.day_name[dia_semana.weekday()]
    dict_save['macaddr'] = dict_payload['macAddr']
    dict_save['gateway'] = dict_payload['gwid']
    dict_save['sinal_ruido'] = str(dict_payload['snr'])
    dict_save['sinal'] = str(dict_payload['rssi'])

    #Remove chaves vazias do dicionario
    remover_vazio = []
    for chave, valor in dict_save.items():
        if dict_save[chave] == []:
            remover_vazio.append(chave)
    for item in remover_vazio:
        dict_save.pop(item)
    print(dict_save)    
    queue.send_message(MessageBody=str(json.dumps(dict_save, ensure_ascii=False)))
#programa principal:
try:
    print('[STATUS] Inicializando MQTT...')
#inicializa MQTT:
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.username_pw_set(Usuario, Senha)
    client.connect(Broker, PortaBroker, KeepAliveBroker)
    client.loop_forever()

except KeyboardInterrupt:
    print ('\nCtrl+C pressionado, encerrando aplicacao e saindo...')
    sys.exit(0)
