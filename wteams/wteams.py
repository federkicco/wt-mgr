from webexteamssdk import WebexTeamsAPI
from webexteamssdk.exceptions import ApiError
from pprint import pprint
import argparse
import re
import asyncio
from tabulate import tabulate
import csv

api = WebexTeamsAPI()

EURL_BOT_MAIL = "eurl@sparkbot.io"
EURL_BOT_ID = "Y2lzY29zcGFyazovL3VzL1BFT1BMRS8wZjcwMDc1NC1iZDAxLTRiZjMtODMwZC0wZTZmNGEwZGQ0ODY"
MAIL_CSV_PATH = "../mail/mail.csv"


def get_eurl_id():
    eurls = api.people.list(email=EURL_BOT_MAIL)
    for eurl in eurls:
        print(eurl.id)
    return


def team_mail_by_name(team_name):
    class_name = team_name.split()[-1]
    if "Medie" in team_name:
        prefix = "medie"
    elif "Elementari" in team_name:
        prefix = "elem"
    else:
        return
    return f"{prefix}_{class_name}@aurigatech.it".lower()


def add_mail_to_teams(teams_list):
    for team in teams_list:
        team_name = team.name
        e_mail = team_mail_by_name(team.name)
        pprint(f"Adding {e_mail} to team: {team_name}")
        try:
            api.team_memberships.create(team.id, personEmail=e_mail, isModerator=True)
        except ApiError as e:
            print(f"Error: {e}")


def add_mail_to_team(team, mail, moderator=False):
    team_name = team.name
    pprint(f"Adding {mail} to team: {team_name}{' as moderator' if moderator else ''}")
    try:
        api.team_memberships.create(team.id, personEmail=mail, isModerator=moderator)
    except ApiError as e:
        print(f"Error: {e}")


def add_eurl_to_rooms(rooms_list):
    for room in rooms_list:
        room_name = room.title
        pprint(f"Adding {EURL_BOT_MAIL} to room: {room_name}")
        try:
            api.memberships.create(room.id, personEmail=EURL_BOT_MAIL, isModerator=True)
        except ApiError as e:
            print(f"Error: {e}")


def make_eurl_rooms_moderator(rooms_list):
    for room in rooms_list:
        room_name = room.title
        pprint(f"Updating {EURL_BOT_MAIL} as room moderator: {room_name}")
        try:
            eurl_memberships = api.memberships.list(roomId=room.id, personId=EURL_BOT_ID)
        except ApiError:
            print(f"Error: {e}")
        try:
            for eurl_membership in eurl_memberships:
                api.memberships.update(membershipId=eurl_membership.id, isModerator=True)
        except ApiError as e:
            print(f"Error: {e}")


def config_rooms_eurl(rooms_list):
    for room in rooms_list:
        room_name = room.title
        messages = [
            "list off",
            "internal off",
            "url"
        ]

        for msg in messages:
            pprint(f"Sending '{msg}' to room: {room_name}")
            try:
                api.messages.create(room.id, markdown=f"<@personId:{EURL_BOT_ID}|EURL> {msg}")
            except ApiError as e:
                print(f"Error: {e}")


async def clean_msgs_room(room, msg):
    pprint(f"Deleting '{msg.id}' from room: {room.title}")
    try:
        api.messages.delete(msg.id)
    except ApiError as e:
        print(f"Error: {e}")
    return


async def clean_msgs_rooms(rooms_list):
    for room in rooms_list:
        room_name = room.title
        messages = api.messages.list(room.id)

        for msg in messages:
            await clean_msgs_room(room.id, msg)
    return


async def send_msg(room_id, msg):
    try:
        api.messages.create(room_id, markdown=msg)
    except ApiError as e:
        print(f"Error: {e}")


async def get_last_eurl_msg(room_id, wait=2):
    try:
        msgs = None
        while not msgs:
            msgs = [ m for m in api.messages.list(room_id) if m.personId == EURL_BOT_ID ]
            if not msgs:
                await asyncio.sleep(wait)
        return msgs[0].text
    
    except ApiError as e:
        print(f"Error: {e}")

    return

async def get_room_to_url_map(rooms_list):
    url_maps = []
    for room in rooms_list:
        room_name = room.title
        msg = f"<@personId:{EURL_BOT_ID}|EURL> url"
        await send_msg(room.id, msg)
        last_response = await get_last_eurl_msg(room.id)
        url = last_response[last_response.find("https"):]
        url_maps.append({"room": room_name, "url": url})

    return url_maps


def create_teams(in_csv, mail_filter=None):
    mail_field = "Email Address [Required]"
    entries = []
    with open(in_csv) as csv_f:
        csv_out = csv.reader(csv_f, delimiter=",", )
        csv_header = next(csv_out)
        for row in csv_out:
            entry = dict(zip(csv_header, row))
            entry_mail = entry.get(mail_field)
            if mail_filter and mail_filter not in entry_mail:
                continue
            team_name = entry_mail.split("@")[0]
            team_name = team_name.replace("_", " ")
            school, class_name = team_name.split()
            team_name = f"{school.capitalize()} {class_name.upper()}"
            print(f"{team_name} -- {entry_mail}")

            try:
                new_team = api.teams.create(team_name)
                add_mail_to_team(new_team, entry_mail, moderator=True)
            except ApiError as e:
                print(f"Error: {e}")


def get_teams_membership(teams_list, filter=False):
    memberships = []
    for team in teams_list:
        try:
            members = api.team_memberships.list(teamId=team.id)
            for member in members:
                entry = {
                    "team_name": team.name,
                    "member_name": member.personDisplayName,
                    "member_mail": member.personEmail
                }
                memberships.append(entry)
        except ApiError as e:
            # print(f"Error: {e}")
            pass

    if filter:
        memberships = [ 
            m for m in memberships if 
            (
                "eurl" not in m.get("member_mail") 
                and "aurigatech.it" not in m.get("member_mail")
                and "ccepdemo.com" not in m.get("member_mail")
                ) 
            ]

    return memberships


async def main():
    teams_list = [ team for team in api.teams.list()]
    teams = { team.id: team.name for team in teams_list }
    rooms = [ room for room in api.rooms.list() if ("MyTrial" not in room.title and "Prova" not in room.title) and room.type == "group" ]

    #await clean_msgs_rooms(rooms)
    
    # url_maps = await get_room_to_url_map(rooms)
    # print(tabulate(sorted(url_maps, key = lambda i: i["room"]), headers="keys"))

    # create_teams(MAIL_CSV_PATH, mail_filter="liceo")

    member_map = get_teams_membership(teams_list, filter=True)

    print(tabulate(member_map, headers="keys"))

    return
        
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    