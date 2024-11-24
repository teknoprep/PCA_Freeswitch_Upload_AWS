# PCA_Freeswitch_Upload_AWS
This code takes your Freeswitch / FusionPBX call recordings and uploads them to S3 for processing utilizing the Post Call Analytics AI analysis which is located on github as well
https://github.com/aws-samples/amazon-transcribe-post-call-analytics/

## File Recording Info
# File Format
I have the current FreeSWITCH / FusionPBX call recordings setup like this

<code>record_name=${domain_name}_CUST_${caller_id_number}_GUID_${uuid}_AGENT_${destination_number}_DATETIME_${strftime(%Y)}-${strftime(%m)}-${strftime(%d)}T${strftime(%H)}-${strftime(%M)}-${strftime(%S)}_ROUTE_Follow_.${record_ext}</code>

This creates an mp3 recording with the neccessary info and is for ONLY inbound calls... for outbound calls I utilize this string

<code>record_name=${domain_name}_CUST_${caller_destination}_GUID_${uuid}_AGENT_${sip_from_user_stripped}_DATETIME_${strftime(%Y)}-${strftime(%m)}-${strftime(%d)}T${strftime(%H)}-${strftime(%M)}-${strftime(%S)}_ROUTE10_.${record_ext}</code>

At the end of the file name you can see that I am defining the ROUTE of the call... This helps identify to me a way to figure out where the system started recording this call.

Here is an example of a filename that would work well with the PCA defaults

<code>pbx.example.com_CUST_8005551212_GUID_27d0ae14-f943-4585-b06a-df9ea94ccad6_AGENT_201_DATETIME_2024-11-23T15-57-25_ROUTE10_.mp3</code>


# File Location
We are utilizing the folder format that was setup within FusionPBX for FreeSWITCH... This script does not dynamically change for other systems and would need to be modified


# File Length 
I have setup a way to check the length of the file in seconds so that we do not upload files that are too short... especially empty mp3's which cause issues with PCA



## Example .env file is inside the ai_copy.py file
You must create a .env file... This is where you will store your variables... An example file is included in the ai_copy.py in the top comments
