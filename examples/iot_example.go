//go:build example
// +build example

package main

import (
	"fmt"
	"time"

	"github.com/wbrown/janus-datalog/datalog"
)

func main() {
	// Example: IoT sensor data modeling
	fmt.Println("IoT Sensor Data Example")
	fmt.Println("=======================\n")
	
	// Sensor entities
	tempSensor1 := datalog.NewIdentity("sensor:temp:kitchen:1")
	tempSensor2 := datalog.NewIdentity("sensor:temp:bedroom:1")
	motionSensor := datalog.NewIdentity("sensor:motion:entrance:1")
	gateway := datalog.NewIdentity("gateway:home:1")

	// Location entities
	kitchen := datalog.NewIdentity("location:kitchen")
	bedroom := datalog.NewIdentity("location:bedroom")
	entrance := datalog.NewIdentity("location:entrance")
	
	// Base time for our readings
	baseTime := time.Date(2024, 6, 19, 14, 0, 0, 0, time.UTC)
	
	// Static facts about sensors
	baseTx := uint64(baseTime.Unix())
	sensorFacts := []datalog.Datom{
		// Temperature sensor 1
		{E: tempSensor1, A: datalog.NewKeyword(":sensor/type"), V: "temperature", Tx: baseTx},
		{E: tempSensor1, A: datalog.NewKeyword(":sensor/location"), V: kitchen, Tx: baseTx},
		{E: tempSensor1, A: datalog.NewKeyword(":sensor/unit"), V: "celsius", Tx: baseTx},
		{E: tempSensor1, A: datalog.NewKeyword(":sensor/gateway"), V: gateway, Tx: baseTx},

		// Temperature sensor 2
		{E: tempSensor2, A: datalog.NewKeyword(":sensor/type"), V: "temperature", Tx: baseTx},
		{E: tempSensor2, A: datalog.NewKeyword(":sensor/location"), V: bedroom, Tx: baseTx},
		{E: tempSensor2, A: datalog.NewKeyword(":sensor/unit"), V: "celsius", Tx: baseTx},
		{E: tempSensor2, A: datalog.NewKeyword(":sensor/gateway"), V: gateway, Tx: baseTx},

		// Motion sensor
		{E: motionSensor, A: datalog.NewKeyword(":sensor/type"), V: "motion", Tx: baseTx},
		{E: motionSensor, A: datalog.NewKeyword(":sensor/location"), V: entrance, Tx: baseTx},
		{E: motionSensor, A: datalog.NewKeyword(":sensor/gateway"), V: gateway, Tx: baseTx},

		// Location facts
		{E: kitchen, A: datalog.NewKeyword(":location/name"), V: "Kitchen", Tx: baseTx},
		{E: bedroom, A: datalog.NewKeyword(":location/name"), V: "Master Bedroom", Tx: baseTx},
		{E: entrance, A: datalog.NewKeyword(":location/name"), V: "Front Entrance", Tx: baseTx},
	}
	
	fmt.Println("Sensor Configuration:")
	for _, d := range sensorFacts {
		fmt.Printf("  %s\n", d)
	}
	
	// Time-series sensor readings
	fmt.Println("\nSensor Readings (Time-Series):")
	
	// Temperature readings every 5 minutes
	readings := []datalog.Datom{}
	for i := 0; i < 5; i++ {
		readTime := baseTime.Add(time.Duration(i*5) * time.Minute)
		
		// Kitchen temperature
		readings = append(readings, datalog.Datom{
			E:  tempSensor1,
			A:  datalog.NewKeyword(":reading/value"),
			V:  22.5 + float64(i)*0.3, // Gradually warming
			Tx: uint64(readTime.Unix()),
		})

		// Bedroom temperature
		readings = append(readings, datalog.Datom{
			E:  tempSensor2,
			A:  datalog.NewKeyword(":reading/value"),
			V:  21.0 - float64(i)*0.2, // Gradually cooling
			Tx: uint64(readTime.Unix()),
		})

		// Motion detection (only at certain times)
		if i == 1 || i == 3 {
			readings = append(readings, datalog.Datom{
				E:  motionSensor,
				A:  datalog.NewKeyword(":reading/detected"),
				V:  true,
				Tx: uint64(readTime.Unix()),
			})
		}
	}
	
	for _, d := range readings {
		fmt.Printf("  %s\n", d)
	}
	
	// Alert entities based on conditions
	fmt.Println("\nAlerts Generated:")

	alert1 := datalog.NewIdentity("alert:high-temp:kitchen:2024-06-19T14:20:00")
	alertTime := baseTime.Add(20 * time.Minute)
	alertTx := uint64(alertTime.Unix())

	alertDatoms := []datalog.Datom{
		{E: alert1, A: datalog.NewKeyword(":alert/type"), V: "high_temperature", Tx: alertTx},
		{E: alert1, A: datalog.NewKeyword(":alert/sensor"), V: tempSensor1, Tx: alertTx},
		{E: alert1, A: datalog.NewKeyword(":alert/threshold"), V: 23.5, Tx: alertTx},
		{E: alert1, A: datalog.NewKeyword(":alert/value"), V: 23.7, Tx: alertTx},
		{E: alert1, A: datalog.NewKeyword(":alert/message"), V: "Kitchen temperature exceeds threshold", Tx: alertTx},
	}
	
	for _, d := range alertDatoms {
		fmt.Printf("  %s\n", d)
	}
	
	fmt.Println("\nExample Queries for IoT Data:")
	fmt.Println("=============================")
	
	fmt.Println(`
1. Get latest temperature from all sensors:
   [:find ?sensor ?location ?value ?time
    :where [?sensor :sensor/type "temperature" _]
           [?sensor :sensor/location ?loc _]
           [?loc :location/name ?location _]
           [?sensor :reading/value ?value ?time]
    :order-by ?time :desc]

2. Find rooms where motion was detected:
   [:find ?location ?time
    :where [?sensor :sensor/type "motion" _]
           [?sensor :sensor/location ?loc _]
           [?loc :location/name ?location _]
           [?sensor :reading/detected true ?time]]

3. Temperature trends over time:
   [:find ?location ?time ?value
    :where [?sensor :sensor/type "temperature" _]
           [?sensor :sensor/location ?loc _]
           [?loc :location/name ?location _]
           [?sensor :reading/value ?value ?time]
           [(>= ?time 2024-06-19T14:00:00)]
           [(<= ?time 2024-06-19T14:30:00)]
    :order-by ?location ?time]

4. Sensors with alerts:
   [:find ?sensor ?alert-type ?message ?time
    :where [?alert :alert/sensor ?sensor ?time]
           [?alert :alert/type ?alert-type _]
           [?alert :alert/message ?message _]]

5. Cross-sensor correlations:
   [:find ?temp ?motion-time
    :where [?temp-sensor :sensor/type "temperature" _]
           [?motion-sensor :sensor/type "motion" _]
           [?temp-sensor :sensor/location ?loc _]
           [?motion-sensor :sensor/location ?loc _]
           [?temp-sensor :reading/value ?temp ?t1]
           [?motion-sensor :reading/detected true ?motion-time]
           [(within-minutes ?t1 ?motion-time 5)]]
`)
	
	fmt.Println("\nThis same Datalog engine handles IoT data just as naturally as")
	fmt.Println("financial data, social networks, or any other domain!")
}
