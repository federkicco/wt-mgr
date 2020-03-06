from webexteamssdk import WebexTeamsAPI
from webexteamssdk.exceptions import ApiError
from distutils.util import strtobool
import pandas as pd
from pprint import pprint
import argparse
import re
import asyncio
from tabulate import tabulate
import csv
import constants

import logging


api = WebexTeamsAPI()

all_teams = [ team for team in api.teams.list()]
teams_by_id = { team.id: team.name for team in all_teams }
teams_by_name = { team.name: team for team in all_teams }
all_rooms = [ room for room in api.rooms.list() ]


def create_teams(teams_df, add_mail=True, team_filter=None):
    """
    Process the input DF file and create Teams if not yet existing.
    """
    teams_set = teams_df.team_name.unique()

    for team_name in teams_set:
        print(f"Processing team '{team_name}'")
        create_team(team_name)

    if add_mail:
        active_mail_df = teams_df[teams_df.is_active == True]
        print(active_mail_df)


    # for row in csv_out:
    #     entry = dict(zip(csv_header, row))
    #     team_name = entry.get(constants.TEAM_NAME_FIELD)
    #     team_descr = entry.get(constants.TEAM_DESCR_FIELD)

    #     # skip team if not in filter
    #     if team_filter and constants.TEAM_NAME_FIELD not in team_name:
    #         logging.debug(f"Filter: Skipping team {team_name}")
    #         continue

 
    #     # if mail_addr and is_active, add mail
    #     if add_mail:
    #         try:
    #             mail_addr = entry.get(constants.MAIL_ADDR_FIELD)
    #             is_moderator = strtobool(entry.get(constants.MODERATOR_FIELD))
    #             is_active = strtobool(entry.get(constants.ACTIVE_FIELD))

    #             if constants.MAIL_ADDR_FIELD and is_active:
    #                 add_mail_to_team(
    #                     team, 
    #                     mail_addr, 
    #                     is_moderator=is_moderator
    #                     )
    #         except ApiError as e:
    #             print(f"Error: {e}")
    #             continue           

def create_team(team_name):
    team = teams_by_name.get(team_name)
    if not team:
        try:
            api.teams.create(team_name)
        except ApiError as e:
            print(f"Error deleting team {team_name}: {e}")
    else:
        print(f"Team '{team_name}' already exists.")


def delete_teams(teams_df, team_filter=None):
    """
    Read the input DF file and delete Teams.
    """
    teams_set = teams_df.team_name.unique()

    for team_name in teams_set:
        print(f"Processing team '{team_name}'")
        delete_team(team_name)


def delete_team(team_name):
    team = teams_by_name.get(team_name)
    if team:
        try:
            api.teams.delete(team.id)
        except ApiError as e:
            print(f"Error deleting team {team_name}: {e}")
    else:
        print(f"Team {team_name} not found.")


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


def add_mail_to_team(mail, team, is_moderator=False):
    team_name = team.name
    pprint(f"Adding {mail} to team: {team_name}{' as moderator' if is_moderator else ''}")
    try:
        api.team_memberships.create(team.id, personEmail=mail, isModerator=is_moderator)
    except ApiError as e:
        print(f"Error: {e}")


def add_mail_to_teams(mail, teams_list, is_moderator=False):
    for team in teams_list:
        add_mail_to_team(mail, team, is_moderator)
    return


def add_eurl_to_rooms(rooms_list):
    for room in rooms_list:
        room_name = room.title
        pprint(f"Adding {constants.EURL_BOT_MAIL} to room: {room_name}")
        try:
            api.memberships.create(room.id, personEmail=constants.EURL_BOT_MAIL, isModerator=True)
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
                api.messages.create(room.id, markdown=f"<@personId:{constants.EURL_BOT_ID}|EURL> {msg}")
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
        messages = api.messages.list(room.id)
        for msg in messages:
            await clean_msgs_room(room.id, msg)
    return


async def send_msg(room_id, msg):
    try:
        api.messages.create(room_id, markdown=msg)
    except ApiError as e:
        print(f"Error: {e}")
    return


async def get_last_eurl_msg(room_id, wait=2):
    try:
        msgs = None
        while not msgs:
            msgs = [ m for m in api.messages.list(room_id) if m.personId == constants.EURL_BOT_ID ]
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
        msg = f"<@personId:{constants.EURL_BOT_ID}|EURL> url"
        await send_msg(room.id, msg)
        last_response = await get_last_eurl_msg(room.id)
        url = last_response[last_response.find("https"):]
        url_maps.append({"room": room_name, "url": url})

    return url_maps


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-tf", "--teams-file")
    parser.add_argument("-ct", "--create-teams", action="store_true")
    parser.add_argument("-dt", "--delete-teams", action="store_true")
    parser.add_argument("-se", "--skip-email", action="store_false")
    parser.add_argument("--team-filter")
    options = parser.parse_args()

    if options.teams_file:
        teams_df = pd.read_csv(options.teams_file) 
    
        if options.create_teams:
            logging.info("Creating Teams")
            create_teams(teams_df, options.skip_email, options.team_filter)

        if options.delete_teams:
            delete_teams(teams_df)

    #await clean_msgs_rooms(rooms)
    
    # url_maps = await get_room_to_url_map(rooms)
    # print(tabulate(sorted(url_maps, key = lambda i: i["room"]), headers="keys"))

    # create_teams(MAIL_CSV_PATH, mail_filter="liceo")

    # member_map = get_teams_membership(all_teams, filter=True)
    # print(tabulate(member_map, headers="keys"))

    return
        
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    