# atm_caba_bot
Telegram bot to find closest atms

# Usage
The bot recognizes two commands. "Link" and "Banelco". You may choose the one you need and then you should share your location. 

After that the bot will return the three available atms closest  you (in a range of 500m) with their distances from your location. 
The bot will also return an google maps image.

# Considerations
The atms do not have unlimited cash. So the bot suposses that every morning at 8am each atm are recargados with $1000. 
The bot assumes the following:
*If atms returned are 3:
- 70% times people go to closest atm
- 20% to second one.
- 10% to the last one.
*If atms returned are 2:
- 80% chance people go to the closest atm.
- 20% chance people go to the second one.

Each extraction substract $1 from atm.
