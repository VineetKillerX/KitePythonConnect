import json
from SlackUtil import sendMessage
with open('./common/mappings.json', 'r') as f:
    mapping_json = json.load(f)


def setTemplate(token,action,price,reason,algorith):
    template ={
            "type": "section",
            "fields": [
                {
                    "type": "text",
                    "text": "Stock Name :"
                },
                {
                    "type": "text",
                    "text": "Action : "
                },
                {
                    "type": "text",
                    "text": "Price: "
                },
                {
                    "type": "text",
                    "text": "Reason : "
                },
                {
                    "type": "text",
                    "text": "Algorithm: "
                }
            ]
        }
    name = mapping_json[token]
    print(name)
    list_map=[name,action,price,reason,algorith]
    counter=0
    for j in template['fields']:
        j['text'] = j['text'] + str(list_map[counter])
        counter=counter+1
    return template

sendMessage(setTemplate('341249','BUY','200','R','S'))


