import json
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Initialize Kafka producer with two brokers
kafka_brokers = ['b-1.flexitwinkafkacluster.ybm00h.c4.kafka.ap-south-1.amazonaws.com:9092', 'b-2.flexitwinkafkacluster.ybm00h.c4.kafka.ap-south-1.amazonaws.com:9092']
producer = KafkaProducer(
    bootstrap_servers=kafka_brokers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: v.encode('utf-8'),
    acks=1,  # To reduce wait time for acknowledgments
    retries=5,  # Number of retry attempts for sending messages
    linger_ms=10  # Time to wait for batching messages (can be adjusted)
)

def lambda_handler(event, context):
    try:
        # Extract data from the incoming event
        device_data_list = json.loads(event['body'])
        
        # Ensure the data is a list
        if not isinstance(device_data_list, list):
            raise ValueError("JSON must be an array of data")

        # Iterate through each device data in the array
        for device_data in device_data_list:
            # Ensure 'uniqueId' is present in each JSON object
            if 'uniqueId' not in device_data:
                raise ValueError("Each JSON object must contain 'uniqueId'")
            
            # Transform data
            transformed_data = transform_data(device_data)
            
            # Send data to Kafka
            future = producer.send('device.activity.NewCan', partition=0, key='alldata', value=transformed_data)
            
            # Get metadata to ensure the message was sent
            record_metadata = future.get(timeout=10)

    except KafkaError as e:
        # Handle Kafka errors
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Failed to send data',
                'error': str(e)
            })
        }
    except Exception as e:
        # Handle general errors
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'An error occurred',
                'error': str(e)
            })
        }
        
    producer.flush()
    
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Data successfully sent'})
    }


# Mapping VehicleState enum values to their names
vehicle_state_map = {
    0: "Idle",
    1: "Discharging",
    2: "FastChargingEVQ",
    3: "Balancing",
    4: "Alerts",
    5: "UltraFastChargingGB_T",
    6: "FastChargingSoltera",
    7: "FastChargingAther",
    8: "LowPowerMode",
    9: "FastChargingTVS",
    10: "ChargingRapidtron",
    11: "ChargingNeenjas"
}


def map_vehicle_state_to_movement_status(vehicle_state):
    state_name = vehicle_state_map.get(vehicle_state, "nogps")
    if state_name in ["Idle", "Alerts"]:
        return "idle"
    elif state_name in ["FastChargingEVQ", "FastChargingSoltera", "UltraFastChargingGB_T", "FastChargingTVS", "FastChargingAther", "ChargingRapidtron", "ChargingNeenjas"]:
        return "charging"
    elif state_name == "Discharging":
        return "moving"
    return "nogps"
    

def transform_data(data):
    can_data = data['evCanData']
    vehicle_state = can_data.get('VehicleState')
    movement_status = map_vehicle_state_to_movement_status(vehicle_state)

    transformed_data = {
        'uniqueId': data['uniqueId'],
        'vehicleName': data['vehicleName'],
        'timeStamp': data['timestamp'],
        'latitude': data['latitude'],
        'longitude': data['longitude'],
        'speed': data.get('speed'),
        'movementStatus': movement_status,
        'evCanData': {
            'SOC': can_data.get('SOC'),
            'batteryId': can_data.get('BatteryID'),
            'OverVoltage': 1 if can_data.get('ErrorState1') == 1 else 0,
            'Undervoltage': 1 if can_data.get('ErrorState1') == 2 else 0,
            'OverCurrent': 1 if can_data.get('ErrorState1') == 4 else 0,
            'OverTemp': 1 if can_data.get('ErrorState1') == 16 else 0,
            'shortCircuitError': 1 if can_data.get('ErrorState2') == 32 else 0,
            'HSC_Low': can_data.get('CurrentCSA'),
            'Vstack': can_data.get('Vstack'),
            'Temp1': can_data.get('T1'),
            'Temp2': can_data.get('T2'),
            'Temp3': can_data.get('T3'),
            'Temp4': can_data.get('T4'),
            'Temp5': can_data.get('T5'),
            'Temp6': can_data.get('T6'),
            'Temp7': can_data.get('T7'),
            'Temp8': can_data.get('T8'),
            'Temp9': can_data.get('T9'),
            'Temp10': can_data.get('T10'),
            'Temp11': can_data.get('T11'),
            'Temp12': can_data.get('T12'),
            'Temp13': can_data.get('T13'),
            'Temp14': can_data.get('T14'),
            'Temp15': can_data.get('T15'),
            'Temp16': can_data.get('T16'),
            'V1': can_data.get('C1'),
            'V2': can_data.get('C2'),
            'V3': can_data.get('C3'),
            'V4': can_data.get('C4'),
            'V5': can_data.get('C5'),
            'V6': can_data.get('C6'),
            'V7': can_data.get('C7'),
            'V8': can_data.get('C8'),
            'V9': can_data.get('C9'),
            'V10': can_data.get('C10'),
            'V11': can_data.get('C11'),
            'V12': can_data.get('C12'),
            'V13': can_data.get('C13'),
            'V14': can_data.get('C14'),
            'V15': can_data.get('C15'),
            'V16': can_data.get('C16'),
            'ChargerComError': can_data.get('ChargerError1'),
            'FetShortCircuitError': 1 if can_data.get('ErrorState1') == 32 else 0,
            'FullCapacity': can_data.get('RatedCapacity'),
            'chargerTimeout': 1 if can_data.get('ErrorState2') == 4 else 0,
            'Ready': 1 if can_data.get('ErrorState1') == 128 else 0,
            'VehicleState': vehicle_state,
            'SwVersion': can_data.get('SWVersionMajor'),
            'HSC_HI': can_data.get('CurrentAFE'),
            'FetOverTemp': 1 if can_data.get('ErrorState1') == 16 else 0,
            'FanStatus': can_data.get('FANStatus'),
            'Vmin': can_data.get('Cvmin'),
            'Vmax': can_data.get('Cvmax'),
            'ChargeETA': can_data.get('ChargeETA')
        }
    }
    return transformed_data