#include <iostream>
#include <cstdlib>
#include <chrono>
#include <thread>
#include <unistd.h>
#include <bson.h>
#include <mongoc.h>
#include "json.hpp" 
#include "mqtt/client.h" 
#include <deque>
#include <map>
#include <vector>

#define QOS 1
#define BROKER_ADDRESS "tcp://localhost:1883"

using namespace std;

bool sensor = false; // Leitura no sensor
bool alarm_op = false; // Alarme sinalizada

string sensorID_;
string machineID_;
string dataType_;

deque<double> sensorDataCPU;
deque<long long> sensorDataRAM;
vector<thread> threads; // Vetor de threads
map<string, int> alarms; // Map de alarmes 
map<string, int> intervalMap; // Map de intervalos sensores
map<string, string> lastData; // Armazena última data

vector<string> split(const string &str, char delim) {
    vector<string> tokens;
    string token;
    istringstream tokenStream(str);
    while (getline(tokenStream, token, delim)) {
        tokens.push_back(token);
    }
    return tokens;
}

string convertToISOString(const tm& timeStruct) {
    ostringstream oss;
    oss << std::put_time(&timeStruct, "%Y-%m-%dT%H:%M:%SZ");
    return oss.str();
}

time_t convertToTimeT(const string& dateString) {
    tm timeStruct = {};
    istringstream ss(dateString);
    ss >> std::get_time(&timeStruct, "%Y-%m-%dT%H:%M:%S");
    time_t time = std::mktime(&timeStruct);
    return time;
}

bool timeOff(const string& machineID, const string& sensorID) {
    auto time_now = chrono::system_clock::now();
    time_t time_now_t = std::chrono::system_clock::to_time_t(time_now);
    tm* time_now_tm = std::gmtime(&time_now_t);
    string isoString = convertToISOString(*time_now_tm);
    // Convertendo o valor armazenado em last_data[sensor_id] para um objeto de tempo

    time_t time1 = convertToTimeT(lastData[sensorID]);
    time_t time2 = convertToTimeT(isoString);

    time_t diff = time2 - time1;

    int diffInSeconds = static_cast<int>(diff);

    // Verificando se o sensor está inativo
    if (diffInSeconds > intervalMap[sensorID]) {
        return true;
    }
    return false;
}

void insert_value(mongoc_collection_t *collection, string machineID, string timestamp_str, int value) {
    bson_error_t error;
    bson_oid_t oid;
    bson_t *doc;
    
    tm tm{};
    istringstream ss{timestamp_str};
    ss >> get_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
    time_t time_t_timestamp = mktime(&tm);

    doc = bson_new();
    BSON_APPEND_UTF8(doc, "machineID", machineID.c_str());
    BSON_APPEND_TIME_T(doc, "timestamp", time_t_timestamp);
    BSON_APPEND_INT32(doc, "value", value);

    if (!mongoc_collection_insert_one(collection, doc, NULL, NULL, &error)) {
        cerr << "Failed to insert doc: " << error.message << endl;
    }

    bson_destroy(doc);
}

void insert_alarm(mongoc_collection_t *collection, string machineID, string sensorID, string message) {
    bson_error_t error;
    bson_oid_t oid;
    bson_t *doc;

    doc = bson_new();
    BSON_APPEND_UTF8(doc, "machineID", machineID.c_str());
    BSON_APPEND_UTF8(doc, "sensorID", sensorID.c_str());
    BSON_APPEND_UTF8(doc, "Message", message.c_str());

    if (!mongoc_collection_insert_one(collection, doc, NULL, NULL, &error)) {
        cerr << "Failed to insert doc: " << error.message << endl;
    }

    bson_destroy(doc);
}

void insert_firstMsg(mongoc_collection_t *collection, string machineID, string sensorID, int interval) {
    bson_error_t error;
    bson_oid_t oid;
    bson_t *doc;

    doc = bson_new();
    BSON_APPEND_UTF8(doc, "machineID", machineID.c_str());
    BSON_APPEND_UTF8(doc, "sensorID", sensorID.c_str());
    BSON_APPEND_INT32(doc, "interval", interval);

    if (!mongoc_collection_insert_one(collection, doc, NULL, NULL, &error)) {
        cerr << "Failed to insert doc: " << error.message << endl;
    }

    bson_destroy(doc);
}

double mediaCPU(string machineID) { // Retorna a Média CPU
    double media = 0;
    int size = 0;
    if(sensorDataCPU.size() <= 5)
        return media;
    else{
        for(const auto& i : sensorDataCPU) {
            media = media + i;
            size++;
        } 
        media = media / size;
    }
    return media;
}

long long mediaRAM(string machineID) { // Retorna a Média RAM
    long long media = 0;
    int size = 0;
    if(sensorDataRAM.size() <= 5)
        return media;
    else{
        for(const auto& i : sensorDataRAM) {
            media = media + i;
            size++;
        } 
        media = media / size;
    }
    return media;
}

void alarms_function(){
    string clientID = "clientID_alarms";
    mqtt::client client(BROKER_ADDRESS, clientID);
    
    // Initialize MongoDB client and connect to the database.
    mongoc_init();
    mongoc_client_t *mongodb_client = mongoc_client_new("mongodb://localhost:27017");
    mongoc_database_t *database = mongoc_client_get_database(mongodb_client, "alarms");
    
    // Create an MQTT callback.
    class callback : public virtual mqtt::callback {
        mongoc_database_t *db_a;

    public:
        callback(mongoc_database_t *db_a) : db_a(db_a) {}

        void message_arrived(mqtt::const_message_ptr msg) override {
            auto j = nlohmann::json::parse(msg->get_payload());

            string alarm = "alarms";
            string machineID = j["machineID"];
            string sensorID = j["sensorID"];
            string message = j["message"];

            cout << "MachineID: " << machineID << endl;
            cout << "SensorID: " << sensorID << endl;
            cout << "Alarm: " << message << endl;

            // Get collection and persist the document.
            mongoc_collection_t *collection = mongoc_database_get_collection(db_a, alarm.c_str());
            insert_alarm(collection, machineID, sensorID, message);
            mongoc_collection_destroy(collection);
        }
    };
    callback cb(database);
    client.set_callback(cb);
    

    // Connect to the MQTT broker.
    mqtt::connect_options connOpts;
    connOpts.set_keep_alive_interval(20);
    connOpts.set_clean_session(true);

    string topic;
    topic = "/alarms";
    
    try {
        client.connect(connOpts);
        client.subscribe(topic, QOS);
    } catch (mqtt::exception& e) {
        cerr << "Error: " << e.what() << endl;
    }
    
    while (true) {
        this_thread::sleep_for(chrono::seconds(1));
    }
}

void sensors_function(string sensorID, string machineID, string dataType){
    string sensorID_ = sensorID;
    string machineID_ = machineID;
    string dataType_ = dataType;
    string message;
    alarms[sensorID] = 0;

    std::cout << "SensorID: " << sensorID_ << endl;
    std::cout << "MachineID: " << machineID_ << endl;
    std::cout << "DataType: " << dataType_ << endl;
    

    string clientID = "clientID" + sensorID_;
    mqtt::client client(BROKER_ADDRESS, clientID);
    
    // Initialize MongoDB client and connect to the database.
    mongoc_init();
    mongoc_client_t *mongodb_client = mongoc_client_new("mongodb://localhost:27017");
    mongoc_database_t *database = mongoc_client_get_database(mongodb_client, "sensors_data");
    
    // Create an MQTT callback.
    class callback : public virtual mqtt::callback {
        mongoc_database_t *db_s;

    public:
        callback(mongoc_database_t *db_s) : db_s(db_s) {}

        void message_arrived(mqtt::const_message_ptr msg) override {
            auto j = nlohmann::json::parse(msg->get_payload());

            string topic = msg->get_topic();
            auto topic_parts = split(topic, '/');
            string machineID = topic_parts[1];
            string sensorID = topic_parts[2];

            string timestamp = j["timestamp"];
            double value_CPU;
            long long value_RAM;

            if(sensorID.compare("0x1") == 0) { // CPU
                value_CPU = j["value"];
                std::cout << "value_CPU: " << value_CPU << endl;
                std::cout << "Data: " << timestamp << endl;

                if(value_CPU > mediaCPU(machineID))
                    alarms[sensorID] = 1;
                else{
                     if(sensorDataCPU.size() > 10){
                        sensorDataCPU.pop_front();
                    }
                    sensorDataCPU.push_back(value_CPU);

                    lastData[sensorID] = timestamp;
                    // Get collection and persist the document.
                    mongoc_collection_t *collection = mongoc_database_get_collection(db_s, sensorID.c_str());
                    insert_value(collection, machineID, timestamp, value_CPU);
                    mongoc_collection_destroy(collection);
                }
            }
            else{
                value_RAM = j["value"]; // RAM
                std::cout << "value_RAM: " << value_RAM << endl;
                std::cout << "Data: " << timestamp << endl;
        
                if(value_RAM > mediaRAM(machineID)) 
                    alarms[sensorID] = 1;
                else{
                    if(sensorDataRAM.size() > 10){
                        sensorDataRAM.pop_front();
                    }
                    sensorDataCPU.push_back(value_RAM);

                    lastData[sensorID] = timestamp;
                    // Get collection and persist the document.
                    mongoc_collection_t *collection = mongoc_database_get_collection(db_s, sensorID.c_str());
                    insert_value(collection, machineID, timestamp, value_RAM);
                    mongoc_collection_destroy(collection);
                }      
            }
        }
    };
    callback cb(database);
    client.set_callback(cb);
    
    // Connect to the MQTT broker.
    mqtt::connect_options connOpts;
    connOpts.set_keep_alive_interval(20);
    connOpts.set_clean_session(true);

    string topic;
    topic = "sensors/" + machineID_ + "/" + sensorID_;
    
    try {
        client.connect(connOpts);
        client.subscribe(topic, QOS);
    } catch (mqtt::exception& e) {
        cerr << "Error: " << e.what() << std::endl;
    }
    
    while (true) {
        this_thread::sleep_for(chrono::seconds(1));
        if(timeOff(machineID, sensorID)){
            alarms[sensorID] = 2;
        }
        if(alarms[sensorID] > 0){
            nlohmann::json alarm;
            alarm["machine_ID"] = machineID;
            alarm["sensorID"] = sensorID;
            if(alarms[sensorID] == 1)
                    alarm["description"] = "Atenção!! Valor anormal!";
                else
                    alarm["description"] = "Atenção!! Falha sensor!";
            
            // Publish the JSON message to the appropriate topic
            string topic = "/alarms";
            mqtt::message msg_alarm(topic, alarm.dump(), QOS, false);
            clog << "message published - topic: " << topic << " - message: " << alarm.dump() << endl;
            client.publish(msg_alarm);

        }
        alarms[sensorID] = 0;
    }
}

int main(int argc, char* argv[]) {
    string clientID;
    mqtt::client client(BROKER_ADDRESS, clientID);

    // Initialize MongoDB client and connect to the database.
    mongoc_init();
    mongoc_client_t *mongodb_client = mongoc_client_new("mongodb://localhost:27017");
    mongoc_database_t *database = mongoc_client_get_database(mongodb_client, "sensors");

    // Create an MQTT callback.
    class callback : public virtual mqtt::callback {
        mongoc_database_t *db;
        
    public:
        callback(mongoc_database_t *db) : db(db) {}

        void message_arrived(mqtt::const_message_ptr msg) override {
            auto j = nlohmann::json::parse(msg->get_payload());
            sensor = true;

            string message = "Initial_msg";
            string machineID = j["machineID"];
            string sensorID = j["sensors"]["sensorID"];
            string  dataType = j["sensors"]["dataType"];
            int interval = j["sensors"]["data_interval"];
            intervalMap[sensorID] = interval;

            sensorID_ = sensorID;
            machineID_ = machineID;
            dataType_ = dataType;

            cout << "Data: " << interval << endl;
            cout << "Data: " << machineID << endl;
        
            // Get collection and persist the document.
            mongoc_collection_t *collection = mongoc_database_get_collection(db, message.c_str());
            insert_firstMsg(collection, machineID, sensorID, interval);
            mongoc_collection_destroy(collection);
        }
    };

    callback cb(database);
    client.set_callback(cb);

    // Connect to the MQTT broker.
    mqtt::connect_options connOpts;
    connOpts.set_keep_alive_interval(20);
    connOpts.set_clean_session(true);

     string topic = "/sensor_monitors";

    try {
        client.connect(connOpts);
        client.subscribe("/sensors/#", QOS);
    } catch (mqtt::exception& e) {
        cerr << "Error: " << e.what() << endl;
        return EXIT_FAILURE;
    }

    while (true) {
        this_thread::sleep_for(chrono::seconds(1));
        if(alarm_op){
            threads.push_back(thread(alarms_function));
            alarm_op = false;  
        }
        if(sensor){
            threads.push_back(thread(sensors_function, ref(sensorID_), ref(machineID_), ref(dataType_)));
            sensor = false;
        }
    }
    
    for (auto& thread : threads) {
            thread.join();
    }

    // Cleanup MongoDB resources
    mongoc_database_destroy(database);
    mongoc_client_destroy(mongodb_client);
    mongoc_cleanup();

    return EXIT_SUCCESS;
}
