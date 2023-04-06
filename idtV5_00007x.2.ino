#include "TonyS_X1.h"
#include "TonyS_X1_ExternalModule.h"

#define REAL_RUN true
///############------------- Define Modbus Device ID ---#################//
#include <ModbusMaster.h>
ModbusMaster node;
void preTransmission() {;}//digitalWrite(RS485_EN, 1);
void postTransmission() {;}//digitalWrite(RS485_EN, 0);

#define ZLAN_MODBUS_IO true
//int PT100_CONVERTER_DEVID = 0; // 100 or 101
int POWER_METER1_DEVID =  10;     //Tiak T330,T250
int POWER_METER2_DEVID =  11;     // Schneider
int OLTC_DEVID =  1; 
int TR42_DEVID =  2;
int WISCO_CONVERTER_DEVID = 20;
int ADAM4055_DEVID = 55;
int modbus_swap=0;
#if ZLAN_MODBUS_IO
int ZLAN_MODBUS_ID = 1;
bool DI_data[8];
bool DO_data[8];
bool DI_data_old[8]={2,2,2,2,2,2,2,2};
bool DO_data_old[8]={2,2,2,2,2,2,2,2};
float AI_data[8];
int DO_register[8] = {17,18,19,20,21,22,23,24};
String msg_z;
#endif
///############--------------  Watchdog ----------------################//
#include "esp_system.h"
hw_timer_t *timer = NULL;
void IRAM_ATTR resetModule() {
  ets_printf("reboot\n");
  ESP.restart();
  //esp_restart_noos();
}
int watchdog_loop = 5;

///############--------------  ANALOG INPUTs  -------------##############//
#define WATER_SENSOR_ANALOG false
#define WATER_SENSOR_MODBUS false
MAX11301 MAX;    //  Call Library's class
uint8_t pinADC[2] = {AIO4, AIO5};
uint8_t ixAn = 0;
//#if WATER_SENSOR_ANALOG
float fWaterLevel_cm=9999.0;
//#endif
float fPT100data=9999.0;


///############--------------  AMBIENT SENSORS ------------##############//
Adafruit_SHT31 sht31 = Adafruit_SHT31();
float ambient_temperature;
float ambient_humidity;


///############--------------  PHERIPHERAL I/O  -----------##############//
#define TX_LED IO7
#define SENT_LED IO4
#define NET_LED IO5
#define DI1 IO6

///############---------------------- GPS  ---------------##############//
#include "SoftwareSerial.h"
SoftwareSerial gpsSerial(IO10,IO11);
bool bGPS_status = false;
String sGPS_lat;
String sGPS_lon;

///############---------------------- RTC  ---------------##############//
RTC_DS3231 rtc;
char daysOfTheWeek[7][12] = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};


///############ WATER via WISCO + Adam4055 ---------------##############//
int HI_WATER_COUNT = 0;
int LO_WATER_COUNT = 0;

bool DI[8];
bool DO[8];

///############---------------------- NB-IOT  ------------##############//
String nbIMEI;
NBIoT nb;
NBIoT_MQTT mqtt;
#define SERVER_IP   "210.1.56.105"
#define SERVER_PORT 1883
#define USER  "cctadmin"
#define PASS  "iotadminsoi21"

String msg;
String msg_h;
///############----------------------TIMERs  ------------##############//
unsigned long previousMillis = 0;
unsigned long readAnalogMillis = 0;
unsigned long readGPSMillis = 0;
unsigned long mqttSentMillis = 0;
unsigned long readAmbientMillis = 0;
unsigned long tx_pinOnStartMills = 0;
unsigned long checkHttptMills = 0;

const long interval = 2000;
uint16_t checkSleep = 500;
int nInitParamTimeout=0;
int nInitTimeout=0;
unsigned long mqttSentHarmMillis = 0;
unsigned long wdtCheckMill = 0;
#define WATCHDOG_TIMER    60000
typedef struct  {
  bool isCal;
  int mid;
  float multiply;
  float adder;
  float offset;
  int bitflag;
} CalParam;

typedef struct  {
  int tapMode;
  int tapPosition;
  int tapCounter;
  int tapCounterOld;
  //bool tapInterface;
  bool OLTC_interface_ready;
} TapController;

//#define CAL_PARAMS_QUANTITY 26
//int useCal[CAL_PARAMS_QUANTITY] = {1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 20, 21,
//                                  22, 23, 24, 25, 128, 129, 130, 198, 199, 200, 16, 350};

#define CAL_PARAMS_QUANTITY 25
int useCal[CAL_PARAMS_QUANTITY] = {1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 20, 21,
                                  22, 23, 24, 25, 128, 129, 130, 198, 199, 200,350};
CalParam CalParams[CAL_PARAMS_QUANTITY];
TapController tapController;

void initParam() {

  for (int i = 0; i < CAL_PARAMS_QUANTITY; i++) {
    CalParams[i].mid = useCal[i];
    CalParams[i].isCal = false;
    CalParams[i].multiply = 1;
    CalParams[i].adder = 0;
  }
}

//default value
int HTTP_INTERVAL = 900;
int REALTIME_INTERVAL = 5;
float RATEOFCHANGE = 5.0;
int RATEOFCHANGE_EN = 0;
int METER_MODEL = 3;
int CT_RATIO = 1;
int PT_RATIO = 1;
int NORMAL_FLOW = 1;
int PROJECT_ID = 0;
char PROJECT_CODE[7];
float RATE_KVA = 500;
int IDTVERSION;
bool MOTORDRIVE_EN = true;
int is_checksure_over = 0;
int is_checksure_under = 0;
bool DRY_TYPE = 0;
int PT100_CONVERTER_DEVID = 0;
int WATER_SESOR_EN = 0;  //0=not install,1=4-20 mA,2=Modbus WISCO
float WATER_LEVEL_HILIMIT;
float WATER_LEVEL_LOLIMIT;

int IDT_INIT_COMPLETE = 0;
int IDT_INIT_PARAMS = 0;

float temperature, humidity;
bool gps_stat=false;
#define LIMIT_I2C_DEVICE 10
#define GPS_STATE_NORMAL 0
#define GPS_STATE_FAIL   1
float lat,  lon;
float lat1,  lon1;

float fAnalog_in[4]; 

float wattOld;
float voltOld[3];
float percentLoadOld;
float percentCheck;

bool realtime_sent = false;
bool http_sent = false;
//bool bModbusToggle = true;
int nNextCheck = 99;
bool mqtt_send_now = false;

void setup()
{

  Serial.begin(115200);
  gpsSerial.begin(9600);
  initParam();

  if (! rtc.begin()) {
    Serial.println("Couldn't find RTC");
    while (1);
  }
  Serial2.begin(9600, SERIAL_8N1, RX2, TX2);   // rx,tx  of modbus
  delay(10);
  Serial.println("IDT v.5 Starting");
  idt.begin();
  delay(10);
  idtGPS.slot(SLOT1);
  idtGPS.begin(9600);
  
  MAX.defaultConfig(); // Config port
  //                      AIO0 = Port , Mode, AVR_INV, RANGE, SAMPLES, ASSOCIATED);       // Read about config above
  MAX.Advance_Config_Port(AIO4,         0x07, 0x00,    0x02,  0x07,    0x00);  // Config Port 0 to ADC 0-2.5V
  MAX.Advance_Config_Port(AIO5,         0x07, 0x00,    0x02,  0x07,    0x00);  // Config Port 0 to ADC 0-2.5V
  Serial.println("Config Port...");
  //                            Pin, Output = 1229(3V)
  //MAX.Basic_Config_Port_For_GPO(AIO4, 1229);  //   Set Pin AIO1 to GPO and Output = 1229(3V) , Maximum = 4095(10V)
  Serial.println("Success !");  
  delay(100);
  idt.pinMode(TX_LED, OUTPUT);
  idt.pinMode(SENT_LED, OUTPUT);
  idt.pinMode(NET_LED, OUTPUT);
  //idt.pinMode(IO6, INPUT);
  MAX.Basic_Config_Port_For_GPI(AIO7, 2785);
  MAX.Basic_Config_Port_For_GPI(AIO6, 2785);


//    while (1){
//      read_t330(10,0,0);
//      delay(10000);
//    }
  
  
  //rtc.adjust(DateTime(2022, 3, 27, 15, 38, 1));
//  node.begin(POWER_METER2_DEVID,Serial2);
//  node.preTransmission(preTransmission);
//  node.postTransmission(postTransmission);

  if (rtc.lostPower()) {
    Serial.println("RTC lost power, lets set the time!");
    // following line sets the RTC to the date & time this sketch was compiled
    rtc.adjust(DateTime(F(__DATE__), F(__TIME__)));
    // This line sets the RTC with an explicit date & time, for example to set
    // January 21, 2014 at 3am you would call:
    //rtc.adjust(DateTime(2022, 1, 3, 4, 12, 1));
    //lib example-> 
    //rtc.adjust(DateTime(2014, 1, 21, 3, 0, 0));
  }

    if (! sht31.begin(0x44)) {   // Set to 0x45 for alternate i2c addr
    Serial.println("Couldn't find SHT31");
    while (1) delay(1);
  }
  else Serial.println("Founded SHT31");
  
#if REAL_RUN
  /* NB-IoT Begin */
  Serial.println("\r\nNBIoT Setting");
  nb.begin(SLOT2);
  Serial.println("NBIoT Ready");
  Serial.print("SIM(S/N) :");
  Serial.println(nb.getSIMSerial());
  Serial.print("IMEI :");
  nbIMEI = nb.getIMEI();
  Serial.println(nbIMEI);
  Serial.print("IMSI :");
  Serial.println(nb.getIMSI());

  if (nbIMEI.length() < 5){delay(5000);ESP.restart();}
  Serial.print("Network Status:");

  int retryConnect=0;
  while (!nb.getNetworkStatus())
  {
    retryConnect++;
    Serial.print(".");
    delay(500);
     idt.digitalWrite(NET_LED, HIGH); 
    delay(500);
    idt.digitalWrite(NET_LED, LOW);

    if (retryConnect>30) ESP.restart();

  }
  
  MAX.writeGPO(AIO4, 1);
  Serial.println("OK");
  Serial.print("Signal :");
  Serial.println(nb.getSignal());
  Serial.print("Signal :");
  Serial.print(nb.getSignaldBm());
  Serial.println("dBm");
  Serial.print("Signal Level :");
  Serial.println(nb.getSignalLevel());
  Serial.print("Device IP :");
  Serial.println(nb.getDeviceIP());

  mqtt.callback = mqttCallBack; 
#endif

  readRTC();
  tx_pinOnStartMills = millis();



  tapController.OLTC_interface_ready = false;
  
  
}

void loop(){
#if REAL_RUN

  if (IDT_INIT_PARAMS){
    timerWrite(timer, 0); //reset timer (feed watchdog)
    //Serial.println("reset timer (feed watchdog)");
    long tme = millis();

  
      if (millis() - wdtCheckMill > WATCHDOG_TIMER){

        Serial.println(",-----------------------------xxxxxxxxxxxxxxx wdt=" + String(watchdog_loop));
        
        if (watchdog_loop <= 0) ESP.restart();
    
        watchdog_loop--;

        wdtCheckMill = millis();
      }
  }
    
  if(!mqtt.connected())
  {
    connectServer();  //connect & subscribe
  }
  else idt.digitalWrite(NET_LED, HIGH);

 while (IDT_INIT_COMPLETE==0){
    mqttGetPid(String(nbIMEI));
    nInitTimeout++;
    Serial.print("nInitTimeout ");Serial.println(nInitTimeout);
    if (nInitTimeout>10) ESP.restart();
    Serial.println("Wait IDT_INIT_COMPLETE");
    delay(2000);
  }
  int q=0;

  while (IDT_INIT_PARAMS==0){
    mqttGetMultipleParams(CAL_PARAMS_QUANTITY,0); 
    nInitParamTimeout++;
    Serial.print("nInitParamTimeout ");Serial.println(nInitParamTimeout);
    if (nInitParamTimeout>10) ESP.restart();
    Serial.println("Wait IDT_INIT_PARAM_COMPLETE");
    delay(500);
    mqttSendDataString("203=1",1);
    //readGPS2();

    //if (q>CAL_PARAMS_QUANTITY) q=0;
  }
#endif

  unsigned long currentMillis = millis();
  if (currentMillis - readAnalogMillis >= 5000) 
  {
    if (WATER_SESOR_EN >0){
      if (WATER_SESOR_EN==1)  fWaterLevel_cm  = MAX.readADC(pinADC[0]);  
      if (WATER_SESOR_EN==2) fWaterLevel_cm  = readWaterModbus(WISCO_CONVERTER_DEVID);

      readAdamModbus(ADAM4055_DEVID);
    }
    
    bool dataGPI = 0;
    dataGPI = MAX.readGPI(AIO6);  //    Read GPI from port 19
    Serial.print("Data GPI AIO6: ");  
    Serial.println(dataGPI);
    dataGPI = MAX.readGPI(AIO7);  //    Read GPI from port 19
    Serial.print("Data GPI AIO7: ");  
    Serial.println(dataGPI);

    read_oltc(OLTC_DEVID);
    if (PT100_CONVERTER_DEVID > 0) fPT100data = read_modbus_pt100(PT100_CONVERTER_DEVID);
    ambient_temperature = sht31.readTemperature();
    ambient_humidity = sht31.readHumidity();    
    readAnalogMillis = currentMillis;
  }


  currentMillis = millis();
  if (currentMillis - previousMillis >= 1500) 
  {

    if (METER_MODEL==1)read_t330(POWER_METER1_DEVID,0,0);
    else if (METER_MODEL==3) read_schneider(POWER_METER2_DEVID);

#if ZLAN_MODBUS_IO
         read_zlan_modbus(ZLAN_MODBUS_ID);
         //msg=msg+msg_z;
#endif

    previousMillis = millis();
  }
  

  if (REAL_RUN){
    currentMillis = millis();
    if (currentMillis - mqttSentMillis >= REALTIME_INTERVAL*1000) 
    {
       mqttSentData(false);
       timerWrite(timer, 0); //reset timer (feed watchdog)
       mqttSentMillis = millis();
    }

    if (METER_MODEL==3){
      if (currentMillis - mqttSentHarmMillis >= REALTIME_INTERVAL*3*1000) 
      {
    
         read_HarmonicSchneider(POWER_METER2_DEVID);

         String tp;
         //if (http_sent)
         //     tp = "/INPUT/PRODUCT/HTTP/" + String(PROJECT_ID) + "/" + PROJECT_CODE;
         //else
              tp = "/INPUT/PRODUCT/" + String(PROJECT_ID) + "/" + PROJECT_CODE;
              
         idt.digitalWrite(SENT_LED, HIGH);
         Serial.println(tp);
         Serial.println(msg_h);
          if(mqtt.publish(tp,msg_h)){
            Serial.println("OK");
            idt.digitalWrite(SENT_LED, LOW);
          }
          else
            Serial.println("Error");
    
         timerWrite(timer, 0); //reset timer (feed watchdog)
         mqttSentHarmMillis = millis();
    
      }
    }
 }
  
  //currentMillis = millis();
  if (currentMillis - checkHttptMills >= 5000) 
  {
   // Serial.println("http_sent ");Serial.println(http_sent);
     checkHttptMills = millis();
      if (!http_sent){
          int mn = readMnRTC();
          //Serial.println("---------------- Clock --------------------- Minute : "+String(mn));
          //Serial.println("------------ End ------------");
          //bool http_sent = false;
          if (mn%(HTTP_INTERVAL/60) ==0){
            if (mn==nNextCheck){
              http_sent = true;
              modbus_swap = 0;
              nNextCheck = mn+(HTTP_INTERVAL/60);
              if (nNextCheck>59) nNextCheck = nNextCheck-60;
  
            }
          }
          if (nNextCheck==99){
              http_sent = true;
              modbus_swap = 0;
              nNextCheck = mn+(HTTP_INTERVAL/60);
              if (nNextCheck>59) nNextCheck = 0;//nNextCheck-60;
              else nNextCheck = mn+((HTTP_INTERVAL/60)-mn%(HTTP_INTERVAL/60));
          }
          Serial.println(nNextCheck);
          mqttSentData(http_sent);
          http_sent = false;
      }
  }

  currentMillis = millis(); 
  if (currentMillis - readGPSMillis >= 62300) 
  {
    readGPSMillis = currentMillis;
    readGPS2();
    
  }

}
void mqttSentData(bool http_sent){
    String tp;
     if (http_sent)
          tp = "/INPUT/PRODUCT/HTTP/" + String(PROJECT_ID) + "/" + PROJECT_CODE;
     else
          tp = "/INPUT/PRODUCT/" + String(PROJECT_ID) + "/" + PROJECT_CODE;

#if ZLAN_MODBUS_IO
     Serial.println(msg_z);
     msg = msg+msg_z;
#endif
     idt.digitalWrite(SENT_LED, HIGH);
     Serial.println(tp);
     Serial.println(msg);
      if(mqtt.publish(tp,msg)){
        Serial.println("OK");
        idt.digitalWrite(SENT_LED, LOW);
      }
      else
        Serial.println("Error");

      if (http_sent){       
         delay(200);
         idt.digitalWrite(SENT_LED, HIGH);
         Serial.println(tp);
         Serial.println(msg);
          if(mqtt.publish(tp,msg_h)){
            Serial.println("OK");
            idt.digitalWrite(SENT_LED, LOW);
          }
          else
            Serial.println("Error");

      }
}

float readWaterModbus(int wiscoDevID){
  uint8_t result;
  node.clearResponseBuffer();
  Serial2.end();delay(20);
  Serial2.begin(9600, SERIAL_8N1, RX2, TX2);   // rx,tx  of modbus
  delay(30);
  //ModbusMaster node1;delay(20);
  node.begin(wiscoDevID, Serial2);delay(20);
  node.preTransmission(preTransmission);
  node.postTransmission(postTransmission);
  Serial.println();
  Serial.println("readHoldingRegisters_modbus-water length = 2");Serial.print("\t");
  //node1.clearResponseBuffer();
  result = node.readInputRegisters(0, 20);//delay(20);

  float ma_water;
  if (result > 0x00) {
    Serial.println("waterlevel--ku8MBResponseTimedOut");
    node.clearResponseBuffer();
    Serial2.end();delay(60);
    Serial2.begin(9600, SERIAL_8N1, RX2, TX2);  
    return 9999;
  }
  else{
        for (int i=0;i<2;i++){
          Serial.print(String(i) + ":");Serial.print(node.getResponseBuffer(i));Serial.print("\t");
          if ((i+1)%8==0)Serial.println();
       }
  }

  union
  {
    uint32_t x;
    float f;
  } u;

  u.x = (((unsigned long)node.getResponseBuffer(1) << 16) | node.getResponseBuffer(0));
  ma_water = u.f;
  
  Serial2.end();delay(60);
  Serial2.begin(9600, SERIAL_8N1, RX2, TX2);  
  Serial.println();Serial.print("water in mA ");Serial.println(ma_water);
  
  //Serial.println(node1.getResponseBuffer(1));
  //0.071875X-0.2875

  int idx = midInCalParam(350);
  float  real_water_cm;
  
  if (idx > -1) {
    if (CalParams[idx].isCal) {
        Serial.println(CalParams[idx].multiply);
        Serial.println(CalParams[idx].adder);
        Serial.println(CalParams[idx].offset);
        
        real_water_cm = ((CalParams[idx].multiply * ma_water) + CalParams[idx].adder) * CalParams[idx].offset;
      
    }
  }
  
  //real_water_cm = ((6.25*ma_water) - 25);
  
  if (real_water_cm >= WATER_LEVEL_HILIMIT+1){
    Serial.println("Water Level Higher....");
    HI_WATER_COUNT++;
    if (HI_WATER_COUNT>=10)
      while (!pumpStartStop(1,ADAM4055_DEVID)){
        delay(2000);
      }
      DI[1] = 1;
        
  } else HI_WATER_COUNT = 0;
  
  if (real_water_cm <= WATER_LEVEL_LOLIMIT-1){
    Serial.println("Water Level Lower....");
    LO_WATER_COUNT++;
    if (LO_WATER_COUNT>=10){
      while (!pumpStartStop(0,ADAM4055_DEVID)){
        delay(2000);
      }
      DI[1] = 0;
      LO_WATER_COUNT = 0;HI_WATER_COUNT = 0;
    }
  }else LO_WATER_COUNT = 0;

  Serial.println();Serial.print("real_water_cm ");Serial.println(real_water_cm);
  return real_water_cm;
}


float readAdamModbus(int AdamDevID){
  uint8_t result;
  node.clearResponseBuffer();
  Serial2.end();delay(20);
  Serial2.begin(9600, SERIAL_8N1, RX2, TX2);   // rx,tx  of modbus
  delay(30);
  ModbusMaster node1;delay(20);
  node.begin(AdamDevID, Serial2);delay(20);
  node.preTransmission(preTransmission);
  node.postTransmission(postTransmission);
  Serial.println();
  Serial.println("readCoils_Adam4055 length = 2");Serial.print("\t");
  //node1.clearResponseBuffer();
  result = node.readCoils(1, 8);//delay(20);

  byte b;
  if (result > 0x00) {
    Serial.println("Adam--ku8MBResponseTimedOut");
    node.clearResponseBuffer();
    Serial2.end();delay(60);
    Serial2.begin(9600, SERIAL_8N1, RX2, TX2);  
    return 9999;
  }
  else{
        for (int i=0;i<8;i++){
          Serial.print(String(i) + ":");Serial.print(node.getResponseBuffer(i));Serial.print("\t");
          if ((i+1)%8==0)Serial.println();
       }
       DI[0] = node.getResponseBuffer(0);
       DI[1] = node.getResponseBuffer(1);
       DI[2] = node.getResponseBuffer(2);
       DI[3] = node.getResponseBuffer(3);
       DI[4] = node.getResponseBuffer(4);
       DI[5] = node.getResponseBuffer(5);
       DI[6] = node.getResponseBuffer(6);
       DI[7] = node.getResponseBuffer(7);
  }

  delay(20);
  //result = node.writeSingleCoil(17, 0);
  //delay(20);
  result = node.readCoils(17, 8);//delay(20);
  if (result > 0x00) {
    Serial.println("Adam--ku8MBResponseTimedOut");
    node.clearResponseBuffer();
    Serial2.end();delay(60);
    Serial2.begin(9600, SERIAL_8N1, RX2, TX2);  
    return 9999;
  }
  else{
        for (int i=0;i<8;i++){
          Serial.print(String(i) + ":");Serial.print(node.getResponseBuffer(i));Serial.print("\t");
          if ((i+1)%8==0)Serial.println();
       }
       DO[0] = node.getResponseBuffer(0);
       DO[1] = node.getResponseBuffer(1);
       DO[2] = node.getResponseBuffer(2);
       DO[3] = node.getResponseBuffer(3);
       DO[4] = node.getResponseBuffer(4);
       DO[5] = node.getResponseBuffer(5);
       DO[6] = node.getResponseBuffer(6);
       DO[7] = node.getResponseBuffer(7);
  }
  
  Serial2.end();delay(60);
  Serial2.begin(9600, SERIAL_8N1, RX2, TX2);  

  //pumpStartStop(1,55);
 // Serial.print(node1.getResponseBuffer(0));
  //Serial.println(node1.getResponseBuffer(1));
  
  return b;
}

bool pumpStartStop(bool StartStop,int AdamDevID){
  uint8_t result;
  node.clearResponseBuffer();
  Serial2.end();delay(20);
  Serial2.begin(9600, SERIAL_8N1, RX2, TX2);   // rx,tx  of modbus
  delay(30);
  ModbusMaster node1;delay(20);
  node.begin(AdamDevID, Serial2);delay(20);
  node.preTransmission(preTransmission);
  node.postTransmission(postTransmission);
  Serial.println();
  Serial.println("writeCoils_Adam4051 length = 2");Serial.print("\t");
  //node1.clearResponseBuffer();
  result = node.writeSingleCoil(17, StartStop);//delay(20);

  byte b;
  if (result > 0x00) {
    Serial.println("Adam--ku8MBResponseTimedOut");
    node.clearResponseBuffer();
    Serial2.end();delay(60);
    Serial2.begin(9600, SERIAL_8N1, RX2, TX2);  
    return 9999;
  }

  return true;
}

float read_modbus_pt100(int devID){
//#if PT100_CONVERTER
  uint8_t result;
  Serial2.end();
  Serial2.begin(9600, SERIAL_8N1, RX2, TX2);   // rx,tx  of modbus
  delay(10);
  ModbusMaster node1;
  node1.begin(devID, Serial2);
  node1.preTransmission(preTransmission);
  node1.postTransmission(postTransmission);
  Serial.println();
  Serial.println("readHoldingRegisters_pt100 length = 2");Serial.print("\t");
  node1.clearResponseBuffer();
  if (PT100_CONVERTER_DEVID == 100) result = node1.readHoldingRegisters(1, 2);
  else if (PT100_CONVERTER_DEVID == 101) result = node1.readHoldingRegisters(20, 2);
  else {
      Serial2.end();
      Serial2.begin(9600, SERIAL_8N1, RX2, TX2);  
      return 9999;
  }
  if (result > 0x00) {
      Serial.println("ku8MBResponseTimedOut");
      timerWrite(timer, 0); //reset timer (feed watchdog)
      Serial2.end();
      Serial2.begin(9600, SERIAL_8N1, RX2, TX2);  
    return fPT100data;
  }
  Serial2.end();delay(60);
  Serial2.begin(9600, SERIAL_8N1, RX2, TX2);  
  Serial.print(node1.getResponseBuffer(0));
  //Serial.println(node1.getResponseBuffer(1));
  float f=9999;
  if (PT100_CONVERTER_DEVID == 100) f = float(node1.getResponseBuffer(0))/10.0;
  if (PT100_CONVERTER_DEVID == 101) f = float(node1.getResponseBuffer(0))/1.0;
  //f = float(node1.getResponseBuffer(0))/1.0;
  Serial.print("f-pt100 ");Serial.println(f);
  return f;

//#endif
}

void control_oltc(int ralse_low) {

  if (!MOTORDRIVE_EN) return;
  
//    Serial2.end();
//    Serial2.begin(9600, SERIAL_8E1, RX2, TX2);   // rx,tx  of modbus
//    delay(10);
//    ModbusMaster node1;
    uint8_t result;
//  ModbusMaster node1;
//    node1.begin(1, Serial2);
//    node1.preTransmission(preTransmission);
//    node1.postTransmission(postTransmission);

  char out_topic[40];
  sprintf(out_topic, "/CALLBACK/ACK/PRODUCT/%d/%s", PROJECT_ID, PROJECT_CODE);
  //Serial.println(out_topic);
 
  if (MOTORDRIVE_EN){
    if (!tapController.OLTC_interface_ready){
      mqtt.publish(out_topic , "OLTC_interface not ready");
    }
    else{
      if (tapController.tapMode == 3){   //External Mode
          Serial.println("Control tap.................  ");
          Serial2.end();
          Serial2.begin(9600, SERIAL_8E1, RX2, TX2);   // tx,rx  of modbus
          delay(10);
          ModbusMaster node1;
          node1.begin(1, Serial2);
          node1.preTransmission(preTransmission);
          node1.postTransmission(postTransmission);
          delay(10);
          result = node1.writeSingleCoil(ralse_low, 1);
        
          Serial2.end();delay(30);
          //Serial2.begin(9600, SERIAL_8N1, RX2, TX2);delay(30);
      }
      else{
         mqtt.publish(out_topic , "OLTC_mode is not set to External");
      }
    }
  }
//  
  //Serial2.end();
  Serial2.begin(9600, SERIAL_8N1, RX2, TX2);delay(30);

//  if (ralse_low){
//    mcp.digitalWrite(0, HIGH);
//    delay(2000);
//    mcp.digitalWrite(0, LOW);
//  }
//  else {
//    mcp.digitalWrite(1, HIGH);
//    delay(2000);
//    mcp.digitalWrite(1, LOW);
//  }
}

void read_oltc(int devID) {
  if (!MOTORDRIVE_EN) return;
  uint8_t result;

  Serial2.end();
  Serial2.begin(9600, SERIAL_8E1, RX2, TX2);   // rx,tx  of modbus
  delay(10);
  ModbusMaster node1;
  node1.begin(devID, Serial2);
  node1.preTransmission(preTransmission);
  node1.postTransmission(postTransmission);

  Serial.print("\nPolling ------  OLTC Data: \n");
  //digitalWrite(red, HIGH);
  result = node1.readHoldingRegisters(0x00, 10);
  //result = node.readHoldingRegisters(40, 8);
  //delay(10);
  Serial.println("readHoldingRegisters_1 0x00 length=" + String(10));
  //Serial.println(result);
  if (result > 0x00) {
    Serial.println("ku8MBResponseTimedOut");
    tapController.OLTC_interface_ready = false;
    Serial2.end();
    Serial2.begin(9600, SERIAL_8N1, RX2, TX2);
  
    return;
  }


  for (int i = 0; i < 10; i++) {
    Serial.print(node1.getResponseBuffer(i)); Serial.print("\t");
  }

  tapController.tapMode = node1.getResponseBuffer(0);
  tapController.tapPosition = node1.getResponseBuffer(1);
  tapController.tapCounter = node1.getResponseBuffer(3);
  //tapController.tapInterface = 0;
  tapController.OLTC_interface_ready = true;
  
  Serial2.end();
  Serial2.begin(9600, SERIAL_8N1, RX2, TX2);
}

void read_schneider(int devID){
  Serial2.end();
  Serial2.begin(9600, SERIAL_8N1, RX2, TX2);   // rx,tx  of modbus
  delay(10);
  node.begin(devID,Serial2);
  node.preTransmission(preTransmission);
  node.postTransmission(postTransmission);

  float Va,Vb,Vc;
  float wattNew;

//  while (1){
//     Serial.println();
//  Serial.println("readHoldingRegisters schneider length=" + String(30));//Serial.print("\t");
//  node.clearResponseBuffer();
//  uint8_t result = node.readHoldingRegisters(3059, 10);
//  int i=0;
//  for (int i=0;i<10;i+=2){
//      if (result == node.ku8MBSuccess){
//        Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));
//      }
//    }
//    Serial.println();
//    delay(5000);
//  }
  Serial.println();
  Serial.println("readHoldingRegisters schneider(2999) length=" + String(64));//Serial.print("\t");
  node.clearResponseBuffer();
  uint8_t result = node.readHoldingRegisters(2999, 64);
  if (result > 0x00) {
    Serial.println("ku8MBResponseTimedOut");
    msg = "data=319==1|";
    if (IDT_INIT_PARAMS==1){
      if (fPT100data < 9999) msg = msg + "20=" + String(fPT100data)  + "|";
      msg = msg + "350=" + String(fWaterLevel_cm)  + "|";
      msg = msg + "21=" + String(ambient_temperature)  + "|";
      msg = msg + "22=" + String(ambient_humidity);
    }
    mqttSentData(false);
    Serial2.end();
    Serial2.begin(9600, SERIAL_8N1, RX2, TX2);  
    return;
  }
  if (result == node.ku8MBSuccess){
//       for (int i=0;i<64;i++){
//        Serial.print(String(i) + ":");Serial.print(node.getResponseBuffer(i));Serial.print("\t");
//        if ((i+1)%8==0)Serial.println();
//       }
      //Serial2.end();delay(10);
      //Serial2.begin(9600, SERIAL_8N1, RX2, TX2);
      Serial.println();
      int i=0;
      int mid=8;
      float z;// = u.f;
      String s;
      msg="";
      z = Word2Float2(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
      float Ia = z;
      msg = "data=" + CombineMsg(z,mid,2);
      
      i=2;mid=9;
      //Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));
      z = Word2Float2(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
      float Ib = z;
      msg = msg + CombineMsg(z,mid,2);
      
      i=4;mid=10;
      //Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));
      z = Word2Float2(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
      float Ic = z;
      msg = msg + CombineMsg(z,mid,2);
      
      i=6;mid=25; //Neutral Current
      //Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));
      z = Word2Float2(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
      msg = msg + CombineMsg(z,mid,2);
      
      i=18;mid=16;  //current Unbalance
      //Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));
      z = Word2Float2(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
      msg = msg + CombineMsg(z,mid,2);
      
      i=20;mid=5;
      //Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));   
      z = Word2Float2(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
      msg = msg + CombineMsg(z,mid,2);

      i=22;mid=6;
      //Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));   
      z = Word2Float2(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
      msg = msg + CombineMsg(z,mid,2);

      i=24;mid=7;
      //Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));   
      z = Word2Float2(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
      msg = msg + CombineMsg(z,mid,2);

      i=28;mid=1;
      //Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));   
      z = Word2Float2(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
      Va = z;
      msg = msg + CombineMsg(z,mid,2);

      i=30;mid=3;
      //Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));   
      z = Word2Float2(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
      Vb = z;
      msg = msg + CombineMsg(z,mid,2);

      i=32;mid=4;
      //Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));   
      z = Word2Float2(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
      Vc = z;
      msg = msg + CombineMsg(z,mid,2);

      i=50;mid=17;
      //Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));   
      z = Word2Float2(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
      msg = msg + CombineMsg(z,mid,2);

      i=60;mid=12;  //watt
      //Serial.print("watt ");
      //Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));   
      z = Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
      wattNew = z;
      msg = msg + CombineMsg(z,mid,2);
      //Serial.println();
      
      z = CarbonCredit(wattNew);
      s = String(z, 2);
      s.trim();
      msg = msg + "1182=" + s + "|";
  
      int i_reverse = 0;
      if (z * NORMAL_FLOW < 0)
        i_reverse = 1;
      msg = msg + "318=" + String(i_reverse) + "|";     

      Serial.println(msg);

    
    wattOld = wattNew;
    voltOld[0] = Va;
    voltOld[1] = Vb;
    voltOld[2] = Vc;
    
      
  }
  Serial.println();
  Serial.println("readHoldingRegisters schneider(3067) length=" + String(30));//Serial.print("\t");
  node.clearResponseBuffer();
  result = node.readHoldingRegisters(3067, 64);

  if (result > 0x00) {
    Serial.println("ku8MBResponseTimedOut");
    Serial2.end();
    Serial2.begin(9600, SERIAL_8N1, RX2, TX2);  
    return;
  }
  if (result == node.ku8MBSuccess){
//       for (int i=0;i<64;i++){
//        Serial.print(String(i) + ":");Serial.print(node.getResponseBuffer(i));Serial.print("\t");
//        if ((i+1)%8==0)Serial.println();
//       }
      int i;
      int mid;
      float z;// = u.f;
      String s;
      i=0;mid=13;  //var
      
      Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));   
      z = Word2Float2(999,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
      msg = msg + CombineMsg(z,mid,2);

      i=8;mid=11;  //VA
      //Serial.print("VA ");
      //Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));   
      z = Word2Float2(999,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
      msg = msg + CombineMsg(z,mid,2);

      mid=18;
      Serial.print(String(RATE_KVA) + " Perform checking percent_load ->\t");
      float percent_load = (z / (RATE_KVA)) * 100.00;
      Serial.println(String(percent_load));
      msg = msg + CombineMsg(percent_load,mid,2);
      percentLoadOld = percent_load;

      if (RATEOFCHANGE_EN == 1) {
      Serial.println("Perform checking RATEOFCHANGE");
      if (checkRateOfChange(percent_load, percentLoadOld)) {
        Serial.println("checkRateOfChange(percent_load,wattOld) = TRUE--------" );
        mqtt_send_now = 1;
      }
      if (checkRateOfChange(wattNew, wattOld)) {
        Serial.println("checkRateOfChange(wattNew,wattOld) = TRUE--------" );
        mqtt_send_now = 1;
      }
      if (checkRateOfChange(Va, voltOld[0])) {
        Serial.println("checkRateOfChange(Va,voltOld[0])) = TRUE--------" );
        mqtt_send_now = 1;
      }
      if (checkRateOfChange(Vb, voltOld[1])) {
        Serial.println("checkRateOfChange(Vb,voltOld[1])) = TRUE--------" );
        mqtt_send_now = 1;
      }
      if (checkRateOfChange(Vc, voltOld[2])) {
        Serial.println("checkRateOfChange(Vc,voltOld[2])) = TRUE--------" );
        mqtt_send_now = 1;
      }
    }
      // i=16;mid=130; //reg 3083
      i=24; mid=130; //reg 3192 //fix 17/3/2023
      //Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));   
      z = Word2Float2(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
      msg = msg + CombineMsg(z,mid,2); 
      
      i=42;mid=19;
      //Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1))); 
      z = Word2Float2(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
      msg = msg + CombineMsg(z,mid,2);
  }

  Serial.println();
  Serial.println("readHoldingRegisters schneider length=" + String(30));//Serial.print("\t");
  node.clearResponseBuffer();
  result = node.readHoldingRegisters(3203, 8);

  if (result > 0x00) {
    Serial.println("ku8MBResponseTimedOut");
    Serial2.end();
    Serial2.begin(9600, SERIAL_8N1, RX2, TX2);  
    return;
  }
  if (result == node.ku8MBSuccess){
//       for (int i=0;i<8;i++){
//        Serial.print(String(i) + ":");Serial.print(node.getResponseBuffer(i));Serial.print("\t");
//        if ((i+1)%8==0)Serial.println();
//       }
      int i;
      int mid;
      float z;// = u.f;
      String s;
      i=0;mid=23;  //kWh import
      //Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));  
      unsigned long l1;
      unsigned long l2;
      unsigned long l;
      l = (unsigned long)node.getResponseBuffer(0)<<48 | (unsigned long)node.getResponseBuffer(i+1)<<32 | (unsigned long)node.getResponseBuffer(i+2)<<16 | node.getResponseBuffer(i+3);
      Serial.println(l);
      z = l/1000.0;
      msg = msg + CombineMsg(z,mid,3); 
      
      i=4;mid=24;
      l = (unsigned long)node.getResponseBuffer(0)<<48 | (unsigned long)node.getResponseBuffer(i+1)<<32 | (unsigned long)node.getResponseBuffer(i+2)<<16 | node.getResponseBuffer(i+3);
      Serial.println(l);
      z = l/1000.0;
      msg = msg + CombineMsg(z,mid,3); 

  }
  
  Serial.println();
  Serial.println("readHoldingRegisters 3769  schneider length=" + String(2));//Serial.print("\t");
  node.clearResponseBuffer();
  result = node.readHoldingRegisters(3769, 2);
  if (result > 0x00) {
    Serial.println("ku8MBResponseTimedOut");
    Serial2.end();
    Serial2.begin(9600, SERIAL_8N1, RX2, TX2);  
    return;
  }
  if (result == node.ku8MBSuccess){
//      for (int i=0;i<2;i++){
//        Serial.print(node.getResponseBuffer(i));Serial.print("\t");
//        if ((i+1)%8==0)Serial.println();
//      }
      int i;
      int mid;
      float z;// = u.f;
      String s;
      i=0;mid=128;  
        //Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));   
        z = Word2Float2(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
        msg = msg + CombineMsg(z,mid,2);
     
  }

//  Serial.println();
//  Serial.println("readHoldingRegisters 3881  schneider length=" + String(2));//Serial.print("\t");
//  node.clearResponseBuffer();
//  result = node.readHoldingRegisters(3881, 2);
//  if (result > 0x00) {
//    Serial.println("ku8MBResponseTimedOut");
//    Serial2.end();
//    Serial2.begin(9600, SERIAL_8N1, RX2, TX2);  
//    return;
//  }
//  if (result == node.ku8MBSuccess){
////      for (int i=0;i<2;i++){
////        Serial.print(node.getResponseBuffer(i));Serial.print("\t");
////        if ((i+1)%8==0)Serial.println();
////      }
//      int i;
//      int mid;
//      float z;// = u.f;
//      String s;
//      i=0;mid=129;  
//        //Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));   
//        z = Word2Float2(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
//        msg = msg + CombineMsg(z,mid,2);
//      
//  }
//  
  msg = msg + CombineMiscellaneousData();
//  node.begin(20, Serial2);
//  node.preTransmission(preTransmission);
//  node.postTransmission(postTransmission);
//  Serial.println();
//  Serial.println("readHoldingRegisters_modbus-water length = 2");Serial.print("\t");
//  node.clearResponseBuffer();
//  result = node.readInputRegisters(30011, 10);
//
//  if (result > 0x00) {
//    Serial.println("ku8MBResponseTimedOut");
//    Serial2.end();delay(60);
//    Serial2.begin(9600, SERIAL_8N1, RX2, TX2);  
//  }
// 
//  
  Serial2.end();delay(10);
  Serial2.begin(9600, SERIAL_8N1, RX2, TX2);

      
}
void read_HarmonicSchneider(int devID){
  Serial2.end();
  Serial2.begin(9600, SERIAL_8N1, RX2, TX2);   // rx,tx  of modbus
  delay(10);
  node.begin(devID,Serial2);
  node.preTransmission(preTransmission);
  node.postTransmission(postTransmission);

//----------------------------- TOTAL Harmonics -------------------------------//
  Serial.println();
  Serial.println("readHoldingRegisters 21299  schneider length=" + String(64));//Serial.print("\t");
  node.clearResponseBuffer();
  uint8_t result = node.readHoldingRegisters(21299, 30);
  if (result > 0x00) {
    Serial.println("ku8MBResponseTimedOut");
    Serial2.end();
    Serial2.begin(9600, SERIAL_8N1, RX2, TX2);  
    return;
  }
  if (result == node.ku8MBSuccess){
      int i;
      int mid;
      float z;// = u.f;
      String s;
      msg_h="";
      i=0;mid=14;  
      //Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));   
      z = Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
      float THDa = z;
      i=2;
      //Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));   
      z = Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
      float THDb = z;   
      i=4;   
      //Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));   
      z = Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
      float THDc = z;
      msg_h = "data=" + CombineMsg((THDa+THDb+THDc)/3.0,mid,2);

      i=22;mid=15;  
      //Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));   
      z = Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
       THDa = z;
      i=2;
      //Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));   
      z = Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
       THDb = z;   
      i=4;   
      //Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));   
      z = Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
       THDc = z;
      msg_h = msg_h + CombineMsg((THDa+THDb+THDc)/3.0,mid,2);
      
  }

  //----------------------------- Voltage Harmonics Group1 -------------------------------//
  Serial.println();
  Serial.println("readHoldingRegisters 21711  schneider length=" + String(64));//Serial.print("\t");
  node.clearResponseBuffer();
  result = node.readHoldingRegisters(21711, 64);
  if (result > 0x00) {
    Serial.println("ku8MBResponseTimedOut");
    Serial2.end();
    Serial2.begin(9600, SERIAL_8N1, RX2, TX2);  
    return;
  }
  if (result == node.ku8MBSuccess){
//      for (int i=0;i<64;i++){
//        Serial.print(node.getResponseBuffer(i));Serial.print("\t");
//        if ((i+1)%8==0)Serial.println();
//      }
      int i;
      int mid;
      float z;// = u.f;
      String s;
      i=0;mid=219;  
      Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));   
      z = Word2Float2(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
      msg_h = msg_h + CombineMsg(z,mid,2);

      for (int i=12;i<72;i+=12){
        mid++;  
        if (mid==221)mid++;
        //Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));   
        z = Word2Float2(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
        msg_h = msg_h + CombineMsg(z,mid,2);
      }      
  }

//----------------------------- Voltage Harmonics Group2 -------------------------------//
  Serial.println();
  Serial.println("readHoldingRegisters 21783  schneider length=" + String(64));//Serial.print("\t");
  node.clearResponseBuffer();
  result = node.readHoldingRegisters(21783, 64);
  if (result > 0x00) {
    Serial.println("ku8MBResponseTimedOut");
    Serial2.end();
    Serial2.begin(9600, SERIAL_8N1, RX2, TX2);  
    return;
  }
  if (result == node.ku8MBSuccess){
//      for (int i=0;i<66;i++){
//        Serial.print(node.getResponseBuffer(i));Serial.print("\t");
//        if ((i+1)%8==0)Serial.println();
//      }
      int i;
      int mid;
      float z;// = u.f;
      String s;
      i=0;mid=225;  
      for (int i=0;i<66;i+=12){
        mid++;  
        //Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));   
        z = Word2Float2(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
        msg_h = msg_h + CombineMsg(z,mid,2);
      }
    
      
  }

//----------------------------- Voltage Harmonics Group3 -------------------------------//
  Serial.println();
  Serial.println("readHoldingRegisters 21855  schneider length=" + String(32));//Serial.print("\t");
  node.clearResponseBuffer();
  result = node.readHoldingRegisters(21855, 48);
  if (result > 0x00) {
    Serial.println("ku8MBResponseTimedOut");
    Serial2.end();
    Serial2.begin(9600, SERIAL_8N1, RX2, TX2);  
    return;
  }
  if (result == node.ku8MBSuccess){
//      for (int i=0;i<42;i++){
//        Serial.print(node.getResponseBuffer(i));Serial.print("\t");
//        if ((i+1)%8==0)Serial.println();
//      }
      int i;
      int mid;
      float z;// = u.f;
      String s;
      i=0;mid=231;  
      for (int i=0;i<48;i+=12){
        mid++;  
        //Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));   
        z = Word2Float2(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
        msg_h = msg_h + CombineMsg(z,mid,2);
      }
    
      
  }

  //----------------------------- Current Harmonics Group1 -------------------------------//
  Serial.println();
  Serial.println("readHoldingRegisters 24427  schneider length=" + String(64));//Serial.print("\t");
  node.clearResponseBuffer();
  result = node.readHoldingRegisters(24427, 64);
  if (result > 0x00) {
    Serial.println("ku8MBResponseTimedOut");
    Serial2.end();
    Serial2.begin(9600, SERIAL_8N1, RX2, TX2);  
    return;
  }
  if (result == node.ku8MBSuccess){
//      for (int i=0;i<64;i++){
//        //Serial.print(node.getResponseBuffer(i));Serial.print("\t");
//        if ((i+1)%8==0)Serial.println();
//      }
      int i;
      int mid;
      float z;// = u.f;
      String s;
      i=0;mid=268;  
      Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));   
      z = Word2Float2(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
      msg_h = msg_h + CombineMsg(z,mid,2);

      for (int i=12;i<72;i+=12){
        mid++;  
        if (mid==221)mid++;
        //Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));   
        z = Word2Float2(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
        msg_h = msg_h + CombineMsg(z,mid,2);
      }      
  }
//----------------------------- Current Harmonics Group2 -------------------------------//
  Serial.println();
  Serial.println("readHoldingRegisters 24499  schneider length=" + String(64));//Serial.print("\t");
  node.clearResponseBuffer();
  result = node.readHoldingRegisters(24499, 64);
  if (result > 0x00) {
    Serial.println("ku8MBResponseTimedOut");
    Serial2.end();
    Serial2.begin(9600, SERIAL_8N1, RX2, TX2);  
    return;
  }
  if (result == node.ku8MBSuccess){
//      for (int i=0;i<64;i++){
//        //Serial.print(node.getResponseBuffer(i));Serial.print("\t");
//        if ((i+1)%8==0)Serial.println();
//      }
      int i;
      int mid;
      float z;// = u.f;
      String s;
      i=0;mid=274;  
      Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));   
      z = Word2Float2(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
      msg_h = msg_h + CombineMsg(z,mid,2);

      for (int i=12;i<72;i+=12){
        mid++;  
        if (mid==221)mid++;
        //Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));   
        z = Word2Float2(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
        msg_h = msg_h + CombineMsg(z,mid,2);
      }      
  }

//----------------------------- Current Harmonics Group2 -------------------------------//
  Serial.println();
  Serial.println("readHoldingRegisters 24571  schneider length=" + String(64));//Serial.print("\t");
  node.clearResponseBuffer();
  result = node.readHoldingRegisters(24571, 64);
  if (result > 0x00) {
    Serial.println("ku8MBResponseTimedOut");
    Serial2.end();
    Serial2.begin(9600, SERIAL_8N1, RX2, TX2);  
    return;
  }
  if (result == node.ku8MBSuccess){
//      for (int i=0;i<64;i++){
//        //Serial.print(node.getResponseBuffer(i));Serial.print("\t");
//        if ((i+1)%8==0)Serial.println();
//      }
      int i;
      int mid;
      float z;// = u.f;
      String s;
      i=0;mid=280;  
      Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));   
      z = Word2Float2(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
      msg_h = msg_h + CombineMsg(z,mid,2);

      for (int i=12;i<72;i+=12){
        mid++;  
        if (mid==221)mid++;
        //Serial.println(Word2Float2(0,node.getResponseBuffer(i),node.getResponseBuffer(i+1)));   
        z = Word2Float2(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
        msg_h = msg_h + CombineMsg(z,mid,2);
      }      
  }

  Serial.println(msg_h);
}

void read_zlan_modbus(int ZLAN_MODBUS_ID){
#if ZLAN_MODBUS_IO
  Serial.println("..........ZLAN_MODBUS_IO........");
  uint8_t result;
  int di_id[8]={619,620,621,622,623,624,625,626};
  int do_id[8]={610,611,612,613,614,615,616,617};
  int ai_id[8]={627,628,629,630,631,632,633,634};
   msg_z = "";
  //char topic[30], topichttp[30];
  delay(50);  
  node.begin(ZLAN_MODBUS_ID,Serial2);
  node.preTransmission(preTransmission);
  node.postTransmission(postTransmission);
  Serial2.flush();
  node.clearResponseBuffer();
  /*### READ DI ###*/
  Serial.println("node.readDiscreteInputs(0,4)");
  result = node.readDiscreteInputs(0,4);
  if (result > 0x00) {
    Serial.println("ku8MBResponseTimedOut DI 1-4");
    //SendMqttStatusMeterFail(319);
    return;
  }
  if (result == node.ku8MBSuccess)
  {
    Serial.print(node.getResponseBuffer(0),HEX);Serial.print(':');
    for (int i=0;i<4;i++){
      DI_data[i] = bitRead(node.getResponseBuffer(0),i);
        Serial.print(DI_data[i]);
        //Serial.print(node.getResponseBuffer(0),i);
        msg_z = msg_z + String(di_id[i]) + "=" + String(DI_data[i]) + "|";
    }
  }

  Serial.print("\t");
  Serial.println("node.readDiscreteInputs(4,4)");
  node.clearResponseBuffer();
  result = node.readDiscreteInputs(4,4);
  if (result > 0x00) {
    Serial.println("ku8MBResponseTimedOut DI 5-8");
    //SendMqttStatusMeterFail(319);
    return;
  }
  if (result == node.ku8MBSuccess)
  {
    Serial.print(node.getResponseBuffer(0),HEX);Serial.print(':');
    for (int i=0;i<4;i++){
      DI_data[i+4] = bitRead(node.getResponseBuffer(0),i);
      Serial.print(DI_data[i+4]);
      msg_z = msg_z + String(di_id[i+4]) + "=" + String(DI_data[i+4]) + "|";
    }
  }
  Serial.print("\t");
  for (int i=0;i<8;i++){
    if (DI_data[i] != DI_data_old[i]) {realtime_sent=true;http_sent=true;}
  }
  for (int i=0;i<8;i++){
    DI_data_old[i] = DI_data[i];
  }
  node.clearResponseBuffer();
  /*### READ DO ###*/
  Serial.println("node.readDiscreteInputs(16,4)");
  result = node.readDiscreteInputs(16,4);
  if (result > 0x00) {
    Serial.println("ku8MBResponseTimedOut DO 1-4");
    //SendMqttStatusMeterFail(319);
    return;
  }
  if (result == node.ku8MBSuccess)
  {
    Serial.print(node.getResponseBuffer(0),HEX);Serial.print(':');
    for (int i=0;i<4;i++){
      DO_data[i] = bitRead(node.getResponseBuffer(0),i);
      Serial.print(DO_data[i]);
      msg_z = msg_z + String(do_id[i]) + "=" + String(DO_data[i]) + "|";
    }
  }
  Serial.print('\t');
  node.clearResponseBuffer();
  Serial.println("node.readDiscreteInputs(20,4)");
  result = node.readDiscreteInputs(20,4);
  if (result > 0x00) {
    Serial.println("ku8MBResponseTimedOut Do 5-8");
    //SendMqttStatusMeterFail(319);
    return;
  }
  if (result == node.ku8MBSuccess)
  {
    Serial.print(node.getResponseBuffer(0),HEX);Serial.print(':');
    for (int i=0;i<4;i++){
      DO_data[i+4] = bitRead(node.getResponseBuffer(0),i);
      Serial.print(DO_data[i+4]);
      msg_z = msg_z + String(do_id[i+4]) + "=" + String(DO_data[i+4]) + "|";
    }
  }
//  for (int i=0;i<8;i++){
//    if (DO_data[i] != DO_data_old[i]) {realtime_sent=true;http_sent=true;}
//  }
//  for (int i=0;i<8;i++){
//    DO_data_old[i] = DO_data[i];
//  }

  //Serial.println(msg_z);
  node.clearResponseBuffer();
//  Serial.println("node.readInputRegisters(0,8)");
//  result = node.readInputRegisters(0,8);
//  if (result > 0x00) {
//    Serial.println("node.readInputRegisters(0,8) ku8MBResponseTimedOut");
//    //SendMqttStatusMeterFail(319);
//    return;
//  }
//  if (result == node.ku8MBSuccess)
//  {
//    Serial.println();
//    Serial.print(node.getResponseBuffer(0),HEX);Serial.print('\t');
////    for (int i=0;i<8;i++){
////      //DO_data[i+4] = bitRead(node.getResponseBuffer(0),i);
////      //Serial.print(DO_data[i+4]);
////      //msg = msg + String(do_id[i+4]) + "=" + String(DO_data[i]) + "|";
////      Serial.print(node.getResponseBuffer(0));Serial.print('\t');
////      AI_data[i] = float(node.getResponseBuffer(0)); // + float(random(1000,1100)) + (float(random(10,99))/100.0);
////      msg = msg + String(ai_id[i]) + "=" + String(AI_data[i]) + "|";
////    }
//     Serial.println();
//  }
//  int str_len = msg.length();
//  msg.remove(str_len-1,1);
//  
//  //int str_len = msg.length() + 1;
//  char char_array[str_len+1];
//  Serial.println("Len:" + String(str_len));
//  msg.toCharArray(char_array, str_len);
//  
//  sprintf(topic, "/INPUT/PRODUCT/%d/%s", PROJECT_ID, PROJECT_CODE);
//  printf(topichttp, "/INPUT/PRODUCT/HTTP/%d/%s", PROJECT_ID, PROJECT_CODE);
//  if (realtime_sent){
//    //Serial.println(msg);
//    mqtt.publish(topic,char_array);
//  }
//  if (http_sent){
//    //Serial.println(msg);
//    mqtt.publish(topichttp,char_array);
//  }  
//  
  //sprintf(topic, "/INPUT/PRODUCT/%d/%s", PROJECT_ID, PROJECT_CODE);
  //sprintf(topichttp, "/INPUT/PRODUCT/HTTP/%d/%s", PROJECT_ID, PROJECT_CODE);

  
  node.clearResponseBuffer();

#endif

}

String CombineMsg(float z,int mid,int dec){
  String s;
  s = String(z,dec);
  s.trim();
  return String(mid) + "=" + s + "|"; 
  
}

void read_t330(int devID,bool mqtt_send, bool http) {
  Serial2.end();
  Serial2.begin(9600, SERIAL_8N1, RX2, TX2);   // rx,tx  of modbus
  delay(10);
  int i;
  uint8_t result;
  int TOTAL_RECORD = 64;
  //float record[TOTAL_RECORD];
  //char val [15];
  char topic[30], topichttp[30]; //,topic2[30],topichttp2[30];
  uint16_t dataval[2];
  bool b_random = false;
  String s;
  char buf[10];
  float temp_c;

  node.begin(devID,Serial2);
  node.preTransmission(preTransmission);
  node.postTransmission(postTransmission);
  
  union
  {
    uint32_t x;
    float f;
  } u;


  Serial.print("\nPolling - General Data: \n");
  //digitalWrite(red, HIGH);
  result = node.readHoldingRegisters(0x1000, TOTAL_RECORD);
  //result = node.readHoldingRegisters(40, 8);
  //delay(10);
  Serial.println("readHoldingRegisters_1 read_t330[1000] length=" + String(TOTAL_RECORD));
  //Serial.println(result);
  if (result > 0x00) {
    Serial.println("ku8MBResponseTimedOut");
    //SendMqttStatusMeterFail(319);
    return;
  }
  if (result == node.ku8MBSuccess)
  {

//      Serial.println("-----------------------------");
//      for (i=0;i<TOTAL_RECORD;i++){
//        //for (i=0;i<8;i++){
//        Serial.print(node.getResponseBuffer(i));Serial.print("\t");
//        if ((i+1)%16==0) Serial.print("\r\n");
//        //record[i].values = (float)node.getResponseBuffer(i);
//        //record[i] = (float)node.getResponseBuffer(i);
//      }

    msg = "";
    int mid;
    int idx;
    i = 0; //Ia
    mid = 8;
    float z;// = u.f;
    z = Word2Float(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
    float Ia = z;
    //dtostrf(record[i],7, 2, z); //// convert float to char
    s = String(z,2);
    s.trim();
    msg = "data=" + String(mid) + "=" + s + "|";

    i += 2; //Va
    mid = 1;
    z = Word2Float(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
    float Va = z;
    msg = msg + String(mid) + "=" + String(z,2) + "|";

    i += 2; //Va-b
    mid = 5;
    z = Word2Float(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
    msg = msg + String(mid) + "=" + String(z,2) + "|";

    i = 14; //Ib
    mid = 9;
    z = Word2Float(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
    float Ib = z;
    msg = msg + String(mid) + "=" + String(z,2) + "|";

    i = 16; //Vb
    mid = 3;
    z = Word2Float(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
    float Vb = z;
    //dtostrf(record[i],7, 2, z); //// convert float to char
    msg = msg + String(mid) + "=" + String(z, 2) + "|";

    i = 18; //Vb-c
    mid = 6;
    z = Word2Float(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
    msg = msg + String(mid) + "=" + String(z, 2) + "|";

    i = 28; //Ic
    mid = 10;
    z = Word2Float(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
    float Ic = z;
    msg = msg + String(mid) + "=" + String(z, 2) + "|";

    i = 30; //Vc
    mid = 4;
    z = Word2Float(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
    float Vc = z;
    msg = msg + String(mid) + "=" + String(z, 2) + "|";
        
    i = 32; //Vc-a
    mid = 7;
    z = Word2Float(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
    msg = msg + String(mid) + "=" + String(z, 2) + "|";

    i = 42; //sigma A
    mid = 198;
    z = Word2Float(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
    msg = msg + String(mid) + "=" + String(z, 2) + "|";

    i = 48;   //  ∑kVA
    mid = 11;
    z = Word2Float(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
    msg = msg + String(mid) + "=" + String(z, 2) + "|";

    Serial.print("Perform checking percent_load ->\t");
    float percent_load = (z / (RATE_KVA)) * 100.00;
    Serial.println(String(percent_load));

    s = String(percent_load, 2);
    s.trim();
    msg = msg + "18=" + s + "|";

    //i=48;       //∑kVA
    i = 50;     //∑kW
    mid = 12;
    z = Word2Float(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
    s = String(z, 2);
    s.trim();
    msg = msg + String(mid) + "=" + s + "|";
    float wattNew = z;

    z = CarbonCredit(wattNew);
    s = String(z, 2);
    s.trim();
    msg = msg + "1182=" + s + "|";

    
    int i_reverse = 0;
    if (z * NORMAL_FLOW < 0)
      i_reverse = 1;
    msg = msg + "318=" + String(i_reverse) + "|";

    i = 52;  //∑kVar
    mid = 13;
    z = Word2Float(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
    msg = msg + String(mid) + "=" + String(z, 2) + "|";

    i = 54;   //Power Factor
    mid = 130;
    z = Word2Float(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
    msg = msg + String(mid) + "=" + String(z, 2) + "|";

    i = 56;     //Frequency
    mid = 19;
    z = Word2Float(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
    msg = msg + String(mid) + "=" + String(z, 2) + "|";

    i = 60;     //import Energy
    //i=58;       //import KVAh
    mid = 23;
    z = Word2Float(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
    msg = msg + String(mid) + "=" + String(z, 2) + "|";

    i = 62;   //Export Energy
    mid = 24;
    z = Word2Float(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
    msg = msg + String(mid) + "=" + String(z, 2) + "|";

//////////////////----------- Part 2 ---------------------------------------///
    delay(60);
    result = node.readHoldingRegisters(0x1048, 2); //1068-->1048 Edit 7/1/2021
    Serial.println("readHoldingRegisters_2 0x1048");
    //Serial.println(result);
    if (result > 0x00) {
      Serial.println("ku8MBResponseTimedOut");
      return;
    }
    if (result == node.ku8MBSuccess)
    {

      //Serial.println("-----------------------------");
      i = 0;    //Neutral Current
      mid = 25;
      z = Word2Float(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
      msg = msg + String(mid) + "=" + String(z, 2) + "|";
    }

//////////////////----------- Part 3 ---------------------------------------///
    delay(60);
    result = node.readHoldingRegisters(4194, 30);
    Serial.println("readHoldingRegisters_3 0x1062");
    //Serial.println(result);
    if (result > 0x00) {
      Serial.println("ku8MBResponseTimedOut");
    }//return;}
    if (result == node.ku8MBSuccess)
    {

      //Serial.println("-----------------------------");
      Serial.print("\r\n");
      for (i = 0; i < 30 + 2; i += 2) {
        //Serial.print(node.getResponseBuffer(i));Serial.print("\t");
        if ((i + 1) % 16 == 0) Serial.print("\r\n");
        //record[i].values = (float)node.getResponseBuffer(i);
        //record[i] = (float)node.getResponseBuffer(i);
        dataval[1] = node.getResponseBuffer(i);
        dataval[0] = node.getResponseBuffer(i + 1);

        u.x = (((unsigned long)dataval[1] << 16) | dataval[0]);

        float z = u.f;

      }
    

      i = 0;  //Demand ∑W
      mid = 128; // 199-->128 Edit 7/1/2021
      z = Word2Float(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
      msg = msg + String(mid) + "=" + String(z, 2) + "|";

      i = 2;  //Peak Demand
      mid = 199; // 128-->199 Edit 7/1/2021
      z = Word2Float(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
      msg = msg + String(mid) + "=" + String(z, 2) + "|";
   
      i = 4;  //Demand ∑A
      mid = 200;
      z = Word2Float(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
      msg = msg + String(mid) + "=" + String(z, 2) + "|";

      i = 6;  //Peak Demand ∑A
      mid = 129;
      z = Word2Float(mid,node.getResponseBuffer(i),node.getResponseBuffer(i+1));
      dataval[1] = node.getResponseBuffer(i);
      dataval[0] = node.getResponseBuffer(i + 1);
      u.x = (((unsigned long)dataval[1] << 16) | dataval[0]);
    
      
      //--------------------- Calibrate -----------------
//      idx = midInCalParam(mid);
//      //Serial.println("idx" + String(idx));
//      z = u.f;
//      if (idx > -1) {
//        if (CalParams[idx].isCal) {
//          z = ((CalParams[idx].multiply * u.f) + CalParams[idx].adder) * CalParams[idx].offset;
//          //Serial.println(z);
//        }
//      }
      //msg = msg + String(mid) + "=" + String(z, 2) + "|";

      i = 26; //THD I
      dataval[1] = node.getResponseBuffer(i);
      dataval[0] = node.getResponseBuffer(i + 1);
      u.x = (((unsigned long)dataval[0] << 16) | dataval[1]);
      z = u.f;
      msg = msg + "14=" + String(z) + "|";

      i = 28; //THD V
      dataval[1] = node.getResponseBuffer(i);
      dataval[0] = node.getResponseBuffer(i + 1);
      u.x = (((unsigned long)dataval[0] << 16) | dataval[1]);
      z = u.f;
      msg = msg + "15=" + String(z) + "|";

    }

    //Cal. Unbalance
    mid = 16;
    idx = midInCalParam(mid);
    if (CalParams[idx].bitflag == 1) {
      if ((Ia + Ib + Ic > 3))
        msg = msg + "16=" + String(Unbalance(Ia, Ib, Ic)) + "|";
      else
        msg = msg + "16=0|";
    }
    else {
      msg = msg + "16=" + String(Unbalance(Ia, Ib, Ic)) + "|";
    }
    msg = msg + "17=" + String(Unbalance(Va, Vb, Vc)) + "|" ;

 
    if (RATEOFCHANGE_EN == 1) {
      Serial.println("Perform checking RATEOFCHANGE");
      if (checkRateOfChange(percent_load, percentLoadOld)) {
        Serial.println("checkRateOfChange(percent_load,wattOld) = TRUE--------" );
        mqtt_send_now = 1;
      }
      if (checkRateOfChange(wattNew, wattOld)) {
        Serial.println("checkRateOfChange(wattNew,wattOld) = TRUE--------" );
        mqtt_send_now = 1;
      }
      if (checkRateOfChange(Va, voltOld[0])) {
        Serial.println("checkRateOfChange(Va,voltOld[0])) = TRUE--------" );
        mqtt_send_now = 1;
      }
      if (checkRateOfChange(Vb, voltOld[1])) {
        Serial.println("checkRateOfChange(Vb,voltOld[1])) = TRUE--------" );
        mqtt_send_now = 1;
        http = 1;
      }
      if (checkRateOfChange(Vc, voltOld[2])) {
        Serial.println("checkRateOfChange(Vc,voltOld[2])) = TRUE--------" );
        mqtt_send_now = 1;
      }
    }
    percentLoadOld = percent_load;
    wattOld = wattNew;
    voltOld[0] = Va;
    voltOld[1] = Vb;
    voltOld[2] = Vc;

    //Serial.println(msg);
  }

  msg = msg + CombineMiscellaneousData();
  Serial2.end();delay(10);
  Serial2.begin(9600, SERIAL_8N1, RX2, TX2);
  
 }

String read_tr42(){
  return "";
}

String CombineMiscellaneousData(){
  int mid,idx;
  float z;
  String msg_s="";
  
    mid = 21;
    idx = midInCalParam(mid);
    //Serial.println("idx" + String(idx));
    z = ambient_temperature;
    if (idx > -1) {
      if (CalParams[idx].isCal) {
        z = ((CalParams[idx].multiply * ambient_temperature) + CalParams[idx].adder) * CalParams[idx].offset;
      }
    }
    msg_s = msg_s + String(mid) + "=" + String(z, 2) + "|";

    mid = 22;
    idx = midInCalParam(mid);
    z = ambient_humidity;
    if (idx > -1) {
      if (CalParams[idx].isCal) {
        z = ((CalParams[idx].multiply * ambient_humidity) + CalParams[idx].adder) * CalParams[idx].offset;
      }
    }
    msg_s = msg_s + String(mid) + "=" + String(z, 2) + "|";

    if (gps_stat == GPS_STATE_NORMAL) {
      msg_s = msg_s + "201=" + sGPS_lat + "|";
      msg_s = msg_s + "202=" + sGPS_lon + "|" ;
    }
//    else {
//      msg_s = msg_s + "201=" + String(lat1, 7) + "|";
//      msg_s = msg_s + "202=" + String(lon1, 7) + "|" ;
//    }
    msg_s = msg_s + "203=" + String(gps_stat) + "|";

    if (IDT_INIT_PARAMS == 1) {
      if (fPT100data<9999.0){
        mid = 20;  //Top Oil Temperature
        idx = midInCalParam(mid);
        z = fPT100data;
        if (idx > -1) {
          if (CalParams[idx].isCal) {
            z = ((CalParams[idx].multiply * fPT100data) + CalParams[idx].adder) * CalParams[idx].offset;
            //z =  fPT100data;
            //Serial.println(z);
          }
        }
        msg_s = msg_s + String(mid) + "=" + String(z, 2) + "|";
      }
    }
//    mid = 350;
//    idx = midInCalParam(mid);
//    z =fWaterLevel_cm;
//    if (idx > -1) {
//      if (CalParams[idx].isCal) {
//        //Serial.println(CalParams[idx].multiply);
//        //Serial.println(CalParams[idx].adder);
//        //Serial.println(CalParams[idx].offset);
//        z = ((CalParams[idx].multiply * fWaterLevel_cm) + CalParams[idx].adder) * CalParams[idx].offset;
//        //z = f_PT100[1]*0.58;
//        //raw_old = fWaterLevel_cm;
//        //Serial.println(z);
//      }
//    }
//    msg_s = msg_s + String(mid) + "=" + String(z, 4) + "|";

//XXXXXXXXXXXXXXXXXXXX    msg_s = msg_s + "352=" + String(f_PT100[2]) + "|";
    
    //msg_s = msg_s + "20=" + String(f_PT100[0]) + "|";

    //msg_s = msg_s + "124=" + String(digitalRead(AC220_PIN))+ "|";

//XXXXXXXXXXXXXXXXXX    msg_s = msg_s + "125=" + String(nIdtLowBattery) + "|";

    msg_s = msg_s + "319=0|";

    if (WATER_SESOR_EN > 0){
      msg_s = msg_s + "350=" + String(fWaterLevel_cm, 4) + "|";
      msg_s = msg_s + "619=" + String(DI[0]) + "|";
      msg_s = msg_s + "620=" + String(DI[1]) + "|";
      msg_s = msg_s + "621=" + String(DI[2]) + "|";
      msg_s = msg_s + "622=" + String(DI[3]) + "|";
      msg_s = msg_s + "623=" + String(DI[4]) + "|";
      msg_s = msg_s + "624=" + String(DI[5]) + "|";
      msg_s = msg_s + "625=" + String(DI[6]) + "|";
      msg_s = msg_s + "626=" + String(DI[7]) + "|";     
       
      msg_s = msg_s + "610=" + String(DO[0]) + "|";
      msg_s = msg_s + "611=" + String(DO[1]) + "|";
      msg_s = msg_s + "612=" + String(DO[2]) + "|";
      msg_s = msg_s + "613=" + String(DO[3]) + "|";
      msg_s = msg_s + "614=" + String(DO[4]) + "|";
      msg_s = msg_s + "615=" + String(DO[5]) + "|";
      msg_s = msg_s + "616=" + String(DO[6]) + "|";
      msg_s = msg_s + "617=" + String(DO[7]) + "|";
            
    }
    if (MOTORDRIVE_EN){
      if (tapController.OLTC_interface_ready){
          msg_s = msg_s + "339=" + String(tapController.tapMode) + "|";
          msg_s = msg_s + "337=" + String(tapController.tapPosition) + "|";
          msg_s = msg_s + "338=" + String(tapController.tapCounter) + "|";
          msg_s = msg_s + "353=" + String(tapController.OLTC_interface_ready) + "|";
      }
      else{
        msg_s = msg_s + "353=0|";
      }
      
    }
    if (DRY_TYPE==1){
        //Serial.println("---------------------------  TR42-Device Enabled--------------------------- ");
        msg_s = msg_s + read_tr42();
    }

    return msg_s;
}
float Word2Float2(int mid,uint16_t v0,uint16_t v1){
  union
  {
    uint32_t x;
    float f;
  } u;

  float z;
  int idx=0;

  u.x = (((unsigned long)v0 << 16) | v1);
  idx = midInCalParam(mid);
  z = u.f;
  if (idx > -1) {
    if (CalParams[idx].isCal) {
//      Serial.println(CalParams[idx].multiply);
//      Serial.println(CalParams[idx].adder);
//      Serial.println(CalParams[idx].offset);
      z = ((CalParams[idx].multiply * u.f) + CalParams[idx].adder) * CalParams[idx].offset;
      ////Serial.println(z);
    }
    //else {z = u.f;}
  }
  return z;
  
}
float Word2Float(int mid,uint16_t v1,uint16_t v0){
  union
  {
    uint32_t x;
    float f;
  } u;

  float z;
  int idx;

  idx = midInCalParam(mid);
  u.x = (((unsigned long)v0 << 16) | v1);
  z = u.f;
  if (idx > -1) {
    if (CalParams[idx].isCal) {
//      Serial.println(CalParams[idx].multiply);
//      Serial.println(CalParams[idx].adder);
//      Serial.println(CalParams[idx].offset);
      z = ((CalParams[idx].multiply * u.f) + CalParams[idx].adder) * CalParams[idx].offset;
      ////Serial.println(z);
    }
    //else {z = u.f;}
  }
  return z;
}
float Unbalance(float V1, float V2, float V3) {

  Serial.println("Enter Unbalance");
  if ((V1 + V2 + V3) == 0)
    return 0;

  float avrg = (V1 + V2 + V3) / 3.0;
  float difmax = abs(V1 - avrg);
  if (difmax < abs(V2 - avrg))
    difmax = abs(V2 - avrg);
  if (difmax < abs(V3 - avrg))
    difmax = abs(V3 - avrg);

  //Serial.print((difmax/avrg) * 100.0);Serial.print("\t");
  return ((difmax / avrg) * 100.0);
}


int midInCalParam(int mid) {
  for (int i = 0; i < CAL_PARAMS_QUANTITY; i++)
    if (CalParams[i].mid == mid)
      return i;

  return -1;
}

bool checkRateOfChange(float newVal, float OldVal) {

  float dif = abs(newVal - OldVal);

  if (dif == 0) return false;

  float pcent = (dif / OldVal) * 100.0;

  //SerialMon.print(newVal);SerialMon.print(" - ");SerialMon.print(OldVal);SerialMon.print(" = ");SerialMon.print(dif);SerialMon.print("\t");
  //SerialMon.print(pcent);SerialMon.println("\%");

  if (pcent >= (RATEOFCHANGE))
    return true;

  return false;
}

void mqttCallBack(String topic ,char *payload,size_t length)
{
  char out_topic[40];
  char msg_out[128];
  
  Serial.println("<-----Received messages----->");
  Serial.println("Topic--> "+topic);
  Serial.print("Payload-->");
  //Serial.write((uint8_t*)payload,length);
  //Serial.println();
  //Serial.println("<-------------------------->");

  String msg = String(payload);
  Serial.println(msg);
  Serial.println("<-------------------------->");


  int idz = topic.indexOf("/INPUT/PRODUCT/134");
  if (idz>-1){
    //Serial.println("Founded 134XXXXXXXXXXXXXXXX");
    float V1,V2,V3;
    int nx = msg.indexOf("|5=");
    if (nx>0){
      Serial.println("Founded 5=XXXXXXXXXXXXXXXX");
      V1 = (msg.substring(nx+3,nx+8)).toFloat();
      Serial.println(nx);
      Serial.println(V1);
    }
    nx=0;
    nx = msg.indexOf("|6=");
    if (nx>0){
      Serial.println("Founded 6=XXXXXXXXXXXXXXXX");
      V2 = (msg.substring(nx+3,nx+8)).toFloat();
      Serial.println(nx);
      Serial.println(V2);
    }   
    nx=0;
    nx = msg.indexOf("|7=");
    if (nx>0){
      Serial.println("Founded 7=XXXXXXXXXXXXXXXX");
      V3 = (msg.substring(nx+3,nx+8)).toFloat();
      Serial.println(nx);
      Serial.println(V3);
    }   

    float Vavg = (V1+V2+V3)/3.0;
    Serial.println(Vavg);
    if (Vavg < 100) return;
    if (Vavg > 386.5){
      is_checksure_over++;
      if (is_checksure_over > 3){
        //control_oltc(1);
        is_checksure_over = 0;
        is_checksure_under = 0;
      }
    }
    else is_checksure_over = 0;
    if (Vavg < 379.5){
      is_checksure_under++;
      if (is_checksure_under > 3){
        //control_oltc(0);
        is_checksure_over = 0;
        is_checksure_under = 0;
      }
    }
    else is_checksure_under = 0;
  }
  
  sprintf(out_topic, "/CALLBACK/ACK/PRODUCT/%d/%s", PROJECT_ID, PROJECT_CODE);
  Serial.println(out_topic);

  int idy = msg.indexOf("wdt=1");
  if (idy>-1) {
    watchdog_loop = 5;
    return;
  }
  
  idy = msg.indexOf("reset=1");
      //nb.publish(topic,"ACK" + msg,0,1,0);
  if (idy>-1) {Serial.println("reboot...");delay(2000);ESP.restart();}  

  idy = msg.indexOf("data=618=1");
  if (idy>-1) {
    if (MOTORDRIVE_EN){
       mqtt.publish(out_topic , "raise.ack=1"); 
       control_oltc(0);
    }
    else  mqtt.publish(out_topic , "raise.ack=0"); 
    return;
  }
  idy = msg.indexOf("data=618=0");
   if (idy>-1) {
    if (MOTORDRIVE_EN){
       mqtt.publish(out_topic , "lower.ack=1"); 
       control_oltc(1);
    }
    else  mqtt.publish(out_topic , "lower.ack=0"); 
    return;
  }
  


  int idx;
  idx = msg.indexOf("httpGetPid:");
  if (idx>-1) {httpGetPid(msg.substring(11));return;}
  idx = msg.indexOf("httpGpr:");
  if (idx>-1) {httpGetMultipleParams(msg.substring(8));return;} 
  
}


void connectServer()
{
  Serial.println("Open Server...");
  char x_id[10];
  idt.digitalWrite(NET_LED, LOW);
  sprintf(x_id,"id%d",random(1000,100000));
  if(mqtt.open(SERVER_IP,SERVER_PORT))
  {
    Serial.print("Connect...");
    if(mqtt.connect(x_id,USER,PASS))
    {
      Serial.println("OK");
      subscribe();delay(300);
      subscribe_watchdog();
      
      idt.digitalWrite(NET_LED, HIGH);
    }
  }
  else {
    nb.powerOn(0);delay(2500);nb.powerOn(1);
    ESP.restart();}
}

void subscribe_sub_feeder(){
     String sub_feeder_topic="/INPUT/PRODUCT/134/#";
    Serial.print("Subscribe Topic "+ sub_feeder_topic+" : ");
    if(mqtt.subScribe(sub_feeder_topic)){
      Serial.println("OK");
    }
    else
    {
      Serial.println("Error");
      return;
    } 
  
}
void subscribe_watchdog(){
  String wdt_topic="/CALLBACK/WATCHDOG/";
    Serial.print("Subscribe Topic "+ wdt_topic+" : ");
    if(mqtt.subScribe(wdt_topic)){
      Serial.println("OK");
    }
    else
    {
      Serial.println("Error");
      return;
    } 
}

void subscribe_remote_command()
{
  //String topic[3]={"/CCTx/IFC/29/INFO/","test2","test3"};
  String back_topic="/INPUT/PRODUCT/HTTP/" + String(PROJECT_ID) + "/" + PROJECT_CODE;
  Serial.print("Subscribe Topic "+ back_topic+" : ");
    if(mqtt.subScribe(back_topic)){
      Serial.println("OK");
    }
    else
    {
      Serial.println("Error");
      return;
    } 
}
void subscribe()
{
  //String topic[3]={"/CCTx/IFC/29/INFO/","test2","test3"};
  String back_topic="/CALLBACK/SERIAL/"+nbIMEI;
  Serial.print("Subscribe Topic "+ back_topic+" : ");
    if(mqtt.subScribe(back_topic)){
      Serial.println("OK");
    }
    else
    {
      Serial.println("Error");
      return;
    }   
  

  
//  for(uint8_t i=0;i<3;i++)
//  {
//    Serial.print("Subscribe Topic "+ topic[i]+" : ");
//    if(mqtt.subScribe(topic[i]))
//      Serial.println("OK");
//    else
//    {
//      Serial.println("Error");
//      break;
//    }   
//  }
}

 float CarbonCredit(float w){
  float f;


  f= 0.368 * (w*(1-0.1927));

  return f;
}

void mqttGetPid(String imei){
  
    String tp = "/GETHTTPCONFIG/SERIAL/";  // + String(PROJECT_ID) + "/" + PROJECT_CODE;
    idt.digitalWrite(SENT_LED, HIGH);
    mqtt.publish(tp,"httpGetPid:"+imei);
}

bool httpGetPid(String payload) {
  
  idt.digitalWrite(SENT_LED, LOW);
  //payload = "0,0,900,15,15,0,2,150,1,1,868333031256031,51,1c7e0b,2000,4,0,0";
  
  payload = payload + ",";
  Serial.print("full payload    ");Serial.println(payload);
  idt.digitalWrite(NET_LED, HIGH);
  String sa[21];
  int r = 0, t = 0;

  for (int i = 0; i < payload.length(); i++)
  {
    if (payload.charAt(i) == ',')
    {
      sa[t] = payload.substring(r, i);
      r = (i + 1);
      t++;
    }
  }
//
  //for (int i = 0; i < 20; i++) {Serial.print(i);Serial.print(":");Serial.print(sa[i]); Serial.print(", ");}
  Serial.println();

  HTTP_INTERVAL = sa[2].toInt();
  REALTIME_INTERVAL = sa[3].toInt();
  RATEOFCHANGE = sa[4].toFloat();
  RATEOFCHANGE_EN = sa[5].toInt();
  METER_MODEL = sa[6].toInt();
  CT_RATIO = sa[7].toInt();
  PT_RATIO = sa[8].toInt();
  NORMAL_FLOW = sa[9].toInt();
  PROJECT_ID = sa[11].toInt();
  sa[12].toCharArray(PROJECT_CODE, sa[12].length() + 1);
  RATE_KVA = sa[13].toFloat();

  IDTVERSION = sa[14].toInt();
  MOTORDRIVE_EN = sa[15].toInt();
  DRY_TYPE = sa[16].toInt();
  
  if (sa[17].toInt() > 0) PT100_CONVERTER_DEVID = sa[17].toInt();

  sa[18] = sa[18]+":";
  String sb[4];
  r = 0, t = 0;
  for (int i = 0; i < sa[18].length(); i++)
  {
    if (sa[18].charAt(i) == ':')
    {
      sb[t] = sa[18].substring(r, i);
      r = (i + 1);
      t++;
    }
  }
  
  for (int i = 0; i < 3; i++) {Serial.print(i);Serial.print(":");Serial.print(sb[i]); Serial.print(", ");}  
  
  WATER_SESOR_EN = sb[0].toInt();
  if (WATER_SESOR_EN > 0){
    WATER_LEVEL_HILIMIT = sb[1].toFloat();
    WATER_LEVEL_LOLIMIT = sb[2].toFloat();
  }
  
  Serial.print("RATE_KVA-> ");
  Serial.println(RATE_KVA);
  if (RATE_KVA<1.0){
    delay(2000);
    return false;
  }
  Serial.print("PROJECT_CODE-> ");
  Serial.println(PROJECT_CODE);
  //Serial.print("DRY_TYPE-> ");
  //Serial.println(DRY_TYPE);

  Serial.println("HTTP_INTERVAL:" + String(HTTP_INTERVAL));
  if (RATEOFCHANGE_EN == 1) {
    Serial.println("RATEOFCHANGE_EN:" + String(RATEOFCHANGE_EN));
    Serial.println("RATEOFCHANGE_percent:" + String(RATEOFCHANGE));
  }

  if (PROJECT_ID == 0) {
    idt.digitalWrite(NET_LED, LOW);
    IDT_INIT_COMPLETE = 0;
    return false;
  }
  
  Serial.print("WATER_SESOR_EN-> ");Serial.print(WATER_SESOR_EN);Serial.print(" ");
  Serial.print(WATER_LEVEL_HILIMIT);Serial.print(" ");Serial.println(WATER_LEVEL_LOLIMIT);
  
  IDT_INIT_COMPLETE = 1;
  idt.digitalWrite(SENT_LED, HIGH);
  mqttSendSingleData(319,0.5,1);

  String tp = "/CALLBACK/PRODUCT/" + String(PROJECT_ID) + "/" + PROJECT_CODE;
  Serial.print("Subscribe Topic "+ tp +" : ");
    if(mqtt.subScribe(tp)){
      Serial.println("OK");
    }
    else
    {
      Serial.println("Error");
      IDT_INIT_COMPLETE = 0;
      return false;
    }   
    subscribe_sub_feeder();
    
  return true;
  
}

void mqttGetMultipleParams(int quantity,int minus){
  //const char server[] = "cctonline.dyndns.org";
  char resource[256];
  //sprintf(resource, "/api/getmultipleparams/?product_id=%d&code=%s&mids=", PROJECT_ID, PROJECT_CODE);
  sprintf(resource, "httpGpr:product_id=%d&code=%s&mids=", PROJECT_ID, PROJECT_CODE);
//  for (int i = (quantity-minus); i < (quantity); i++) {
//    char b[5];
//    String str;
//    if (i < quantity - 1)
//      str = String(useCal[i]) + ",";
//    else
//      str = String(useCal[i]);
//    str.toCharArray(b, 5);
//    strcat(resource, b);
//    //strcat(resource,c);
//  }
  //for (int i = (0); i < (quantity); i++) {
  for (int i = 0; i < CAL_PARAMS_QUANTITY; i++) {
    char b[5];
    String str;
    if (i < CAL_PARAMS_QUANTITY - 1)
      str = String(useCal[i]) + ",";
    else
      str = String(useCal[i]);
    str.toCharArray(b, 5);
    strcat(resource, b);
    //strcat(resource,c);
  }
  
//  sprintf(resource, "httpGpr:product_id=51&code=1c7e0b&mids=product_id=51&code=1c7e0b&mids=1,3,4");
  Serial.println(resource);
  String tp = "/GETHTTPCONFIG/SERIAL/" + String(PROJECT_ID) + "/" + PROJECT_CODE;
  //String tp = "/CALLBACK/PRODUCT/" + String(PROJECT_ID) + "/" + PROJECT_CODE;  
  idt.digitalWrite(SENT_LED, HIGH);
  mqtt.publish(tp,resource);

}
bool httpGetMultipleParams(String payload) {
  // Server details
  Serial.print("payload 2   ");Serial.println(payload);
  int xx = payload.indexOf('$');
  
  if (xx < 1)
    return false;
  else
    payload.remove(xx);
    
  idt.digitalWrite(SENT_LED, LOW);


  
  //payload = "m1,1,2,ttyS0,1,1,4098,0,1,0|m3,1,2,ttyS0,1,1,4112,0,1,0|m4,1,2,ttyS0,1,1,4126,0,1,0|m5,1,1,ttyS0,1,1,4100,0,1,0|m6,1,1,ttyS0,1,1,4114,0,1,0|m7,1,1,ttyS0,1,1,4128,0,1,0|m8,1,1,ttyS0,1,1,4096,0,1,0|m9,1,1,ttyS0,1,1,4110,0,1,0|m10,1,1,ttyS0,1,1,4124,0,1,0|m11,0.001,1,ttyS0,1,1,4144,0,1,0|m12,0.001,2,ttyS0,1,1,4146,0,1,0|m13,0.001,2,ttyS0,1,1,4148,0,1,0|m16,1,1,ttyS0,0,1,1,1,1,0|m20,1,1,ttyS0,1,2,8208,-3,1,0|m21,0.1,1,ttyS0,,,,0,1,0|m22,1,1,ttyS0,0,0,0,0,1,0|m23,0.001,1,ttyS0,,4098,,0,1,0|m24,0.001,1,ttyS0,,4098,,0,1,0|m25,1,2,ttyS0,,4098,,0,1,0|m128,0.001,2,ttyS0,1,1,1111,0,1,0|m129,1,1,ttyS0,1,1,2222,0,1,0|m130,1,2,ttyS0,1,1,3333,0,1,0|m198,1,1,ttyS0,1,1,4138,0,1,0|m199,0.001,1,ttyS0,,4098,,0,1,0|m200,1,1,ttyS0,,4098,,0,1,0";
  payload = payload + "|";
  


  String sa[CAL_PARAMS_QUANTITY];
  //String sa[NUM_SPLIT_PARAM_INIT];
  int r = 0, t = 0 , t1 = 0;

  for (int i = 0; i < payload.length(); i++)
  {
    if (payload.charAt(i) == '|')
    {
      sa[t] = payload.substring(r, i);
      r = (i + 1);
      t++;
    }
  }

  for (int i = 0; i < CAL_PARAMS_QUANTITY; i++) {
    //for (int i = 0; i < NUM_SPLIT_PARAM_INIT; i++) {
    sa[i] = sa[i] + ",";
    //Serial.print(sa[i]); Serial.println();
    String sa_2[10];
    t1 = 0;
    r = 0;
    payload = sa[i];
    for (int j = 0; j < payload.length(); j++)
    {
      if (payload.charAt(j) == ',')
      {
        sa_2[t1] = payload.substring(r, j);
        //Serial.println("sa_2[t1]=" + String(t1) + " " + sa_2[t1]);
        r = (j + 1);
        t1++;
      }
    }
    
    //Serial.println("_sa_2[0]=" + sa_2[0]);
    int sx = sa_2[0].indexOf('m');
    //Serial.println("sx=" + String(sx));
    sa_2[0].setCharAt(sx, '0');
    String s_mid = sa_2[0];
    //Serial.println("s_mid=" + s_mid);
    //Serial.println("s_mid.toInt()=" + String(s_mid.toInt()));
    int idx = midInCalParam(s_mid.toInt());
    //CalParams[idx].mid = s_mid.toInt();
    //while (1);
    if (idx > -1) {
      //Serial.println("idx=" + String(idx));
      CalParams[idx].multiply = sa_2[1].toFloat();
      CalParams[idx].adder = sa_2[7].toFloat();
      CalParams[idx].offset = sa_2[8].toFloat();
      CalParams[idx].bitflag = sa_2[9].toInt();
      CalParams[idx].isCal = true;

//      Serial.println();
//      Serial.print(CalParams[idx].mid);Serial.print("\t");
//      Serial.print(CalParams[idx].multiply);Serial.print("\t");
//      Serial.print(CalParams[idx].adder);Serial.print("\t");
//      Serial.println(CalParams[idx].offset);Serial.println();

//      if (s_mid.toInt() == 20) {
//        PT100_0_RAW_MAX = sa_2[8].toFloat();
//        PT100_0_RAW_MIN = sa_2[9].toFloat();
//        PT100_0_ADDER = sa_2[7].toFloat();
//        PT100_0_MULTIPLY = sa_2[1].toFloat();
//
//      }
      if (s_mid.toInt() == useCal[CAL_PARAMS_QUANTITY-1]) {
        IDT_INIT_PARAMS=1;
        InitAllTimers();
        idt.digitalWrite(SENT_LED, LOW);
        for (int i = 0; i < CAL_PARAMS_QUANTITY; i++) {
          Serial.print(i); Serial.print("\t");
          Serial.print(CalParams[i].mid); Serial.print("\t");
          Serial.print(CalParams[i].multiply,3); Serial.print("\t");
          Serial.print(CalParams[i].adder); Serial.print("\t");
          Serial.print(CalParams[i].offset); Serial.print("\t");
          Serial.print(CalParams[i].bitflag); Serial.println();
        }
      }
    }
    else 
      Serial.println("Not founded idx by mid");


  }
  Serial.println();

  return true;
}

void InitAllTimers(){
      unsigned long mil = millis();
      mqttSentMillis = mil;
      previousMillis = mil;
      mqttSentHarmMillis = mil;
      wdtCheckMill = mil;
      timer = timerBegin(0, 80, true); //timer 0, div 80
      timerAttachInterrupt(timer, &resetModule, true);
      timerAlarmWrite(timer,  30000000, false); //set time in us
      timerAlarmEnable(timer); //enable interrupt
      
      readGPS2();
}
void mqttSendSingleData(int mid,float val,bool http_sent){
 String msg;
 String tp = "/INPUT/PRODUCT/" + String(PROJECT_ID) + "/" + PROJECT_CODE;
 String tph = "/INPUT/PRODUCT/HTTP/" + String(PROJECT_ID) + "/" + PROJECT_CODE;
    msg = "Data=" + String(mid) + "="  + String(val) + "|";
    Serial.print("http_sent ");Serial.println(http_sent);

    if (http_sent){
      idt.digitalWrite(SENT_LED, HIGH);
      Serial.print("http_sent ");Serial.println(http_sent);
      Serial.println(tph);
      Serial.println(msg);
      if(mqtt.publish(tph,msg)){
        Serial.println("OK");
        idt.digitalWrite(SENT_LED, LOW);
      }
      else
        Serial.println("Error");
    }
            
    Serial.print(msg);Serial.print(tp);
    if(mqtt.publish(tp,msg)){
      Serial.println("OK");
      idt.digitalWrite(SENT_LED, LOW);
    }
    else
      Serial.println("Error");
      

}
void mqttSendDataString(String s,bool http_sent){
 String msg;
 String tp = "/INPUT/PRODUCT/" + String(PROJECT_ID) + "/" + PROJECT_CODE;
 String tph = "/INPUT/PRODUCT/HTTP/" + String(PROJECT_ID) + "/" + PROJECT_CODE;
    msg = "Data=" + s;


    
    Serial.print("http_sent ");Serial.println(http_sent);
    
      if (http_sent){
        idt.digitalWrite(SENT_LED, HIGH);
        Serial.print("http_sent ");Serial.println(http_sent);
        Serial.println(tph);
        Serial.println(msg);
        if(mqtt.publish(tph,msg)){
          Serial.println("OK");
          idt.digitalWrite(SENT_LED, LOW);
        }
        else
          Serial.println("Error");

        //return;
      }

    delay(100);
    Serial.print(msg);Serial.print(tp);
    if(mqtt.publish(tp,msg)){
      Serial.println("OK");
      idt.digitalWrite(SENT_LED, LOW);
    }
    else
      Serial.println("Error");
      
    


}

void mqttSendData(float t,float h,bool http_sent){
 String msg;
 String tp = "/INPUT/PRODUCT/" + String(PROJECT_ID) + "/" + PROJECT_CODE;
 String tph = "/INPUT/PRODUCT/HTTP/" + String(PROJECT_ID) + "/" + PROJECT_CODE;
    msg = "Data=21=" + String(t) + "|22=" + String(h) + "|" ;
    Serial.print("http_sent ");Serial.println(http_sent);
    
      if (http_sent){
        idt.digitalWrite(SENT_LED, HIGH);
        Serial.print("http_sent ");Serial.println(http_sent);
        Serial.println(tph);
        Serial.println(msg);
        if(mqtt.publish(tph,msg)){
          Serial.println("OK");
          idt.digitalWrite(SENT_LED, LOW);
        }
        else
          Serial.println("Error");

        return;
      }

    
    Serial.print(msg);Serial.print(tp);
    if(mqtt.publish(tp,msg)){
      Serial.println("OK");
      idt.digitalWrite(SENT_LED, LOW);
    }
    else
      Serial.println("Error");


}


void LampTest(){
    idt.digitalWrite(NET_LED, HIGH);delay(50);
    idt.digitalWrite(SENT_LED, HIGH);delay(50); 
    idt.digitalWrite(TX_LED, HIGH); delay(50);
    delay(100);
    idt.digitalWrite(NET_LED, LOW);delay(50);
    idt.digitalWrite(SENT_LED, LOW); delay(50);
    idt.digitalWrite(TX_LED, LOW);delay(300);
    idt.digitalWrite(NET_LED, HIGH);
    idt.digitalWrite(SENT_LED, HIGH); 
    idt.digitalWrite(TX_LED, HIGH);delay(400); 
    idt.digitalWrite(NET_LED, LOW);
    idt.digitalWrite(SENT_LED, LOW);
    idt.digitalWrite(TX_LED, LOW); 
}

int readMnRTC(){
    DateTime now = rtc.now();
    Serial.println();
    Serial.print(now.year(), DEC);
    Serial.print('/');
    Serial.print(now.month(), DEC);
    Serial.print('/');
    Serial.print(now.day(), DEC);
    Serial.print(" (");
    Serial.print(daysOfTheWeek[now.dayOfTheWeek()]);
    Serial.print(") ");
    Serial.print(now.hour(), DEC);
    Serial.print(':');
    Serial.print(now.minute(), DEC);
    Serial.print(':');
    Serial.print(now.second(), DEC);
    Serial.println();
    
   Serial.print(now.minute());
    String s = String(now.minute(),DEC);
    
    return s.toInt();

    

    Serial.println();
}



void readRTC(){
      DateTime now = rtc.now();
    
    Serial.print(now.year(), DEC);
    Serial.print('/');
    Serial.print(now.month(), DEC);
    Serial.print('/');
    Serial.print(now.day(), DEC);
    Serial.print(" (");
    Serial.print(daysOfTheWeek[now.dayOfTheWeek()]);
    Serial.print(") ");
    Serial.print(now.hour(), DEC);
    Serial.print(':');
    Serial.print(now.minute(), DEC);
    Serial.print(':');
    Serial.print(now.second(), DEC);
    Serial.println();
    
//    Serial.print(" since midnight 1/1/1970 = ");
//    Serial.print(now.unixtime());
//    Serial.print("s = ");
//    Serial.print(now.unixtime() / 86400L);
//    Serial.println("d");
    
    // calculate a date which is 7 days and 30 seconds into the future
//    DateTime future (now + TimeSpan(7,12,30,6));
//    
//    Serial.print(" now + 7d + 30s: ");
//    Serial.print(future.year(), DEC);
//    Serial.print('/');
//    Serial.print(future.month(), DEC);
//    Serial.print('/');
//    Serial.print(future.day(), DEC);
//    Serial.print(' ');
//    Serial.print(future.hour(), DEC);
//    Serial.print(':');
//    Serial.print(future.minute(), DEC);
//    Serial.print(':');
//    Serial.print(future.second(), DEC);
//    Serial.println();
//    
//    Serial.print("Temperature: ");
//    Serial.print(rtc.getTemperature());
//    Serial.println(" C");
    
    Serial.println();
    //delay(3000);
}

void readGPS2(){
  
  bool newData = false;
  unsigned long chars;
  unsigned short sentences, failed;

  bGPS_status = false;
  // For one second we parse GPS data and report some key values
  for (unsigned long start = millis(); millis() - start < 1000;)
  {
    while (gpsSerial.available())
    {
      char c = gpsSerial.read();
      //Serial.write(c); // uncomment this line if you want to see the GPS data flowing
      if (idtGPS.encode(c)) // Did a new valid sentence come in?
        newData = true;
        
    }
  }
  bGPS_status = newData;
  if (newData)
  {
    float flat, flon;
    unsigned long age;
    idtGPS.f_get_position(&flat, &flon, &age);

    sGPS_lat = String(flat,7);
    sGPS_lon = String(flon,7);
    
    Serial.println();
    Serial.print("LAT=");
    if(flat == IDT5_GPS::GPS_INVALID_F_ANGLE)Serial.print(0.0);
    else Serial.print(flat, 6);

    Serial.print(" LON=");
    if(flon == IDT5_GPS::GPS_INVALID_F_ANGLE)Serial.print(0.0);
    else Serial.print(flon, 6);

    Serial.print(" SAT=");
    if(idtGPS.satellites() == IDT5_GPS::GPS_INVALID_SATELLITES)Serial.print(0);
    else Serial.print(idtGPS.satellites());
  
    Serial.print(" PREC=");
    if(idtGPS.hdop() == IDT5_GPS::GPS_INVALID_HDOP)Serial.print(0);
    else Serial.print(idtGPS.hdop());

    //mqttSendDataString("201=" + sGPS_lat + "|202=" + sGPS_lon + "|203=0",0);
    
  }

  idtGPS.stats(&chars, &sentences, &failed);
  Serial.print(" CHARS=");
  Serial.print(chars);
  Serial.print(" SENTENCES=");
  Serial.print(sentences);
  Serial.print(" CSUM ERR=");
  Serial.println(failed);
  if (chars == 0)
  {
    Serial.println("** No characters received from GPS **");
  }
  checkSleep = chars;
}