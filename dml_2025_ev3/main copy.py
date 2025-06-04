#!/usr/bin/env pybricks-micropython
from pybricks.hubs import EV3Brick
from pybricks.ev3devices import (Motor, TouchSensor, ColorSensor,
                                 InfraredSensor, UltrasonicSensor, GyroSensor)
from pybricks.parameters import Port, Stop, Direction, Button, Color
from pybricks.tools import wait, StopWatch, DataLog
from pybricks.robotics import DriveBase
from pybricks.media.ev3dev import SoundFile, ImageFile


# This program requires LEGO EV3 MicroPython v2.0 or higher.
# Click "Open user guide" on the EV3 extension tab for more information.


# Create your objects here.
ev3 = EV3Brick()


# Write your program here.
ev3.speaker.beep()


# Robot speaks
ev3.speaker.set_speech_options('en', 'm1', 150, 50)
ev3.speaker.set_volume(100)
ev3.speaker.say('''Hi Trayan''')

# Initialize a gripping motor at port A
grip_motor = Motor(Port.A)

# Intialize two motors with default settings on Ports B and C
left_motor = Motor(Port.B)
right_motor = Motor(Port.C)

# Initilize sensors
touch_sensor = TouchSensor(Port.S1)
color_sensor = ColorSensor(Port.S4)
infrared_sensor = InfraredSensor(Port.S3)

# Initialize DriverBase
robot = DriveBase(left_motor, right_motor, wheel_diameter=32, axle_track=177)

# # Move forward
# robot.straight(500)
# # Turn around by 90 degrees
# robot.turn(90)
obstacle = False
distance = robot.distance()
print('Initial distance:', distance)

for i in range(4):
    while not obstacle and robot.distance() - distance < 500:
        # Begin driving forward at 100 millimeters per second.
        robot.drive(100, 0)
        rgb = color_sensor.rgb()
        print('RGB: ' + str(rgb))
        obstacle = touch_sensor.pressed()

    print('Turning on 90 degrees.')
    robot.turn(90)
    distance = robot.distance()
    print('Final distance:', robot.distance())