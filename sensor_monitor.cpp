#include <iostream>
#include <cstdlib>
#include <chrono>
#include <ctime>
#include <thread>
#include <unistd.h>
#include "json.hpp" // json handling
#include "mqtt/client.h" // paho mqtt
#include <iomanip>
#include "sys/types.h"
#include "sys/sysinfo.h"

#define QOS 1
#define BROKER_ADDRESS "tcp://localhost:1883"
#define NUM_SENSORS 2

using namespace std;

// Vetores de threads
vector<thread> threads;

// Obtém o ID da máquina
string get_machineID() {
    // Get the unique machine identifier, in this case, the hostname.
    char hostname[1024];
    gethostname(hostname, 1024);
    string machineID(hostname);

    return machineID;
}

static unsigned long long lastTotalUser, lastTotalUserLow, lastTotalSys, lastTotalIdle;

void init(){
    FILE* file = fopen("/proc/stat", "r");
    fscanf(file, "cpu %llu %llu %llu %llu", &lastTotalUser, &lastTotalUserLow,
        &lastTotalSys, &lastTotalIdle);
    fclose(file);
}

// CPU Usage
double get_CPU_usage() {
double percent;
    FILE* file;
    unsigned long long totalUser, totalUserLow, totalSys, totalIdle, total;

    file = fopen("/proc/stat", "r");
    fscanf(file, "cpu %llu %llu %llu %llu", &totalUser, &totalUserLow,
        &totalSys, &totalIdle);
    fclose(file);

    if (totalUser < lastTotalUser || totalUserLow < lastTotalUserLow ||
        totalSys < lastTotalSys || totalIdle < lastTotalIdle){
        //Overflow detection. Just skip this value.
        percent = -1.0;
    }
    else{
        total = (totalUser - lastTotalUser) + (totalUserLow - lastTotalUserLow) +
            (totalSys - lastTotalSys);
        percent = total;
        total += (totalIdle - lastTotalIdle);
        percent /= total;
        percent *= 100;
    }

    lastTotalUser = totalUser;
    lastTotalUserLow = totalUserLow;
    lastTotalSys = totalSys;
    lastTotalIdle = totalIdle;

    return percent;
}

// RAM Usage
long long get_RAM_usage() {
    struct sysinfo memInfo;
    sysinfo (&memInfo);
    long long physMemUsed = memInfo.totalram - memInfo.freeram;
    // Multiply in next statement to avoid int overflow on right hand side...
    physMemUsed *= memInfo.mem_unit;

    return physMemUsed;
}

int RAM_usage_sensor() {
    string clientID  = "sensor-monitorRAM";
    mqtt::client client(BROKER_ADDRESS, clientID);

    // Connect to the MQTT broker.
    mqtt::connect_options connOpts;
    connOpts.set_keep_alive_interval(20);
    connOpts.set_clean_session(true);

    try {
        client.connect(connOpts);
    } catch (mqtt::exception& e) {
        cerr << "Error: " << e.what() << endl;
        return EXIT_FAILURE;
    }
    clog << "connected to the broker" << endl;

    string machineID = get_machineID();

    int interval = 500; // 500ms
    nlohmann::json j_initial;
    j_initial["machine_id"] = machineID;
    j_initial["sensors"][ "sensor_id"] = "0x2";
    j_initial["sensors"][ "data_type"] = "long long";
    j_initial["sensors"][ "data_interval"] = 0.5; // 0.5s

    // Publish the JSON message to the appropriate topic.
    string topic = "/sensor_monitors";
    mqtt::message msg(topic, j_initial.dump(), QOS, false);
    clog << "message published - topic: " << topic << " - message: " << j_initial.dump() << endl;
    client.publish(msg);

    while(true) {
        // Get the current time in ISO 8601 format.
        auto now = chrono::system_clock::now();
        time_t now_c = chrono::system_clock::to_time_t(now);
        tm* now_tm = localtime(&now_c);
        stringstream ss;
        ss << put_time(now_tm, "%FT%TZ");
        string timestamp = ss.str();

        // Generate a value.
        long long value = get_RAM_usage();

        // Construct the JSON message.
        nlohmann::json j;
        j["timestamp"] = timestamp;
        j["value"] = value;

        // Publish the JSON message to the appropriate topic.
        string topic = "/sensors/" + machineID + "/0x2";
        mqtt::message msg(topic, j.dump(), QOS, false);
        clog << "message published - topic: " << topic << " - message: " << j.dump() << endl;
        client.publish(msg);

        // Sleep for some time.
        this_thread::sleep_for(chrono::milliseconds(interval));
    }
    return EXIT_SUCCESS;
}

int main(int argc, char* argv[]) {
    threads.push_back(thread(RAM_usage_sensor));
    string clientID = "sensor-monitorCPU";
    mqtt::client client(BROKER_ADDRESS, clientID);

    // Connect to the MQTT broker.
    mqtt::connect_options connOpts;
    connOpts.set_keep_alive_interval(20);
    connOpts.set_clean_session(true);

    try {
        client.connect(connOpts);
    } catch (mqtt::exception& e) {
        cerr << "Error: " << e.what() << endl;
        return EXIT_FAILURE;
    }
    clog << "connected to the broker" << endl;

    string machineID = get_machineID();

    int interval = 3000; // 3000ms
    nlohmann::json j_initial;
    j_initial["machine_id"] = machineID;
    j_initial["sensors"][ "sensor_id"] = "0x1";
    j_initial["sensors"][ "data_type"] = "double";
    j_initial["sensors"][ "data_interval"] = 3; // 3s

    // Publish the JSON message to the appropriate topic.
    string topic = "/sensor_monitors";
    mqtt::message msg(topic, j_initial.dump(), QOS, false);
    clog << "message published - topic: " << topic << " - message: " << j_initial.dump() << endl;
    client.publish(msg);

    while(true) {
        // Get the current time in ISO 8601 format.
        auto now = chrono::system_clock::now();
        time_t now_c = chrono::system_clock::to_time_t(now);
        tm* now_tm = localtime(&now_c);
        stringstream ss;
        ss << put_time(now_tm, "%FT%TZ");
        string timestamp = ss.str();

        // Generate a value.
        double value = get_CPU_usage();

        // Construct the JSON message.
        nlohmann::json j;
        j["timestamp"] = timestamp;
        j["value"] = value;

        // Publish the JSON message to the appropriate topic.
        string topic = "/sensors/" + machineID + "/0x2";
        mqtt::message msg(topic, j.dump(), QOS, false);
        clog << "message published - topic: " << topic << " - message: " << j.dump() << endl;
        client.publish(msg);

        // Sleep for some time.
        this_thread::sleep_for(chrono::milliseconds(interval));
    }

     for (auto& thread : threads)
        thread.join();
        
    return EXIT_SUCCESS;
}
