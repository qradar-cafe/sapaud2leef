from datetime import datetime
import re


def extract_sap_properties(record):
    #extract ABAP properties
    record_properties={}
    record_properties["SLGTYPESLGFTYP"] = record[0:1]
    record_properties["SLGTYPEAREA"] = record[1:3]
    record_properties["SLGTYPESUBID"] = record[3:4]
    record_properties["SLGDATTIMDATE"] = record[4:12]
    record_properties["SLGDATTIMTIME"] = record[12:18]
    record_properties["SLGDATTIMDUMMY"] = record[18:20]
    record_properties["SLGPROCUNIXPID"] = record[20:25]
    record_properties["SLGPROCTASKTNO"] = record[25:30]
    record_properties["SLGPROCSLGTTYP"] = record[30:32]
    record_properties["SLGLTRM"] = record[32:40]
    record_properties["SLGUSER"] = record[40:52]
    record_properties["SLGTC"] = record[52:72]
    record_properties["SLGREPNA"] = record[72:112]
    record_properties["SLGMAND"] = record[112:115]
    record_properties["SLGMODE"] = record[115:116]
    record_properties["SLGDATA"] = record[116:180]
    record_properties["SLGLTRM2"] = record[180:200]
    record_properties["payload"] = record
    record_properties["devTimeFormat"] ='MMM dd yy HH:mm:ss' 
    date_time_obj = datetime.strptime(record_properties["SLGDATTIMDATE"]+record_properties["SLGDATTIMTIME"], '%Y%m%d%H%M%S')
    record_properties["devTime"]= date_time_obj.strftime("%b %d %y %H:%M:%S")
    record_properties["usrName"]=record_properties["SLGUSER"]
    try:
        record_properties["src"]=re.search('(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})', record_properties["SLGLTRM2"]).group(1)
    except:
        None

    return record_properties

def create_leef_record(record):
    #LEEF heaader including the event ID
    record_leef = 'LEEF:2.0|SAP|ABAP|1.0|' + record["SLGTYPEAREA"] + record["SLGTYPESUBID"] + '|'
    #Adding each property to the LEEF event
    for key in record:
        if key not in ['Vendor','Product','Version','EventID']:
            record_leef += str(key) + '=' + str(record[key])+'\t'

    return record_leef 


def main():
    print(datetime.now())
    print("Processing File")
    #Open SAP file
    f = open("20220504.AUD", "rb")
    #Create output file
    with open('20220504.leef', 'w') as leef_file:
        pass    
    try:
        counter = 0
        except_count = 0
        #Record size
        record_size = 400

        record = f.read(record_size)
        while len(record) == record_size and counter < 100000000000:
            try:
                counter += 1
                #Parsing record
                record_sap = extract_sap_properties(record.decode('utf-8','backslashreplace').replace('\x00',''))
                #Create the event in LEEF format
                record_leef = create_leef_record(record_sap)
                
                #Write LEEF event into output file
                with open('20220504.leef', 'a') as leef_file:
                    leef_file.write(record_leef+'\n')
                record = f.read(record_size)
            except Exception as e:
                except_count += 1
                print(e)
                print(record)
                print("Record number: " + str(counter))

        print("Last record size: " + str(len(record))+ " (This should be 0)")
        print( str(counter) + " records has been processed with " + str(except_count) + "exceptions" )  
        print(datetime.now())
    finally:
        f.close()

if __name__ == "__main__":
    main()
