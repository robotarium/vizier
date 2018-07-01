The purpose of these files is to experiment with message passing with Vizier.

Instructions:
    1. Run the following in a terminal:
        > mosquitto -p 1844
    2. In separate terminals, run the following scripts:
        > ./test_vizier.sh
        > ./test_a.sh
        > ./test_b.sh

NOTE: If you want to test this over a network, replace the 'localhost' argument with the IP address of the computer hosting the MQTT server.
