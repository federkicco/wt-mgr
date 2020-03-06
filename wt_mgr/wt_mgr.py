import logging
import argparse
import pandas as pd
import numpy as np
import re
import os
import asyncio
from webexteamssdk import WebexTeamsAPI
from webexteamssdk.exceptions import ApiError
from distutils.util import strtobool
from tabulate import tabulate
import constants

logger = logging.getLogger(__name__)

# init WT API connection
api = WebexTeamsAPI()
# pull all known teams and rooms
all_teams = [ team for team in api.teams.list()]
teams_by_id = { team.id: team.name for team in all_teams }
teams_by_name = { team.name: team for team in all_teams }
all_rooms = [ room for room in api.rooms.list() ]


def filter_df(df, options):
    if options.only_inactive:
        df = df[df.is_active == False]
        what = "only inactive"
    elif not options.include_inactive:
        df = df[df.is_active == True]
        what = "only active"
    else:
        what = "all"

    print(f"Processing {what} elements from config.")
    return df


def create_teams(teams_df):
    """
    Process the teams DF and create Teams if not yet existing.
    """
    teams_set = teams_df.team_name.unique()

    for team_name in teams_set:
        print(f"Creating team '{team_name}'")
        create_team(team_name)

    return

      
def create_team(team_name):
    team = teams_by_name.get(team_name)
    if not team:
        try:
            api.teams.create(team_name)
        except ApiError as e:
            print(f"Error deleting team {team_name}: {e}")
    else:
        print(f"Team '{team_name}' already exists.")
    return


def delete_teams(teams_df):
    """
    Read the input DF file and delete Teams.
    """    
    teams_set = teams_df.team_name.unique()

    for team_name in teams_set:
        print(f"Deleting team '{team_name}'")
        delete_team(team_name)
    return


def delete_team(team_name):
    team = teams_by_name.get(team_name)
    if team:
        try:
            api.teams.delete(team.id)
        except ApiError as e:
            print(f"Error deleting team {team_name}: {e}")
    else:
        print(f"Team {team_name} not found.")


def create_rooms(rooms_df):
    """
    Process the rooms DF and create Rooms if not yet existing.
    """
    for _, room in rooms_df.iterrows():
        room_name = room.room_name
        team_name = room.team_name
        print(f"Creating room '{room_name}' in '{team_name}'")
        create_room(room_name, team_name)
    return

      
def create_room(room_name, team_name):
    team = teams_by_name.get(team_name)
    if team:
        try:
            api.rooms.create(room_name, team.id)
        except ApiError as e:
            print(f"Error creating room {room_name}: {e}")
        else:
            print(f"Room '{room_name}' already exists.")


def map_users_to_teams(teams_users_df):
    """
    Map users to teams
    """
    for _, account in teams_users_df.iterrows():
        team = teams_by_name.get(account.team_name)
        if team:
            add_mail_to_team(account.mail_addr, team, account.is_moderator)
    return


def add_mail_to_team(mail, team, is_moderator=False):
    team_name = team.name
    print(f"Adding {mail} to team: {team_name}{' as moderator' if is_moderator else ''}")
    try:
        api.team_memberships.create(team.id, personEmail=mail, isModerator=is_moderator)
    except ApiError as e:
        print(f"Error: {e}")
    return

## old stuff below - review please
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
    # paths and input files
    parser.add_argument("-wd", "--work-dir", required=True)
    parser.add_argument("-tf", "--teams-file")
    parser.add_argument("-rf", "--rooms-file")
    parser.add_argument("-tu", "--teams-users-file")

    # actions
    parser.add_argument("-tc", "--create-teams", action="store_true")
    parser.add_argument("-td", "--delete-teams", action="store_true")
    parser.add_argument("-rc", "--create-rooms", action="store_true")
    parser.add_argument("-rd", "--delete-rooms", action="store_true")
    parser.add_argument("-ua", "--assign-users", action="store_true")
    parser.add_argument("-ur", "--remove-users", action="store_true")

    # options
    parser.add_argument("-ia", "--include-active", action="store_true")
    parser.add_argument("-ii", "--include-inactive", action="store_true")
    parser.add_argument("-oi", "--only-inactive", action="store_true")

    # filters
    parser.add_argument("--team-filter")
    parser.add_argument("--room-filter")
    parser.add_argument("--mail-filter")
    options = parser.parse_args()

    # process the data dir and input config files
    work_dir_override = options.work_dir and os.path.isdir(options.work_dir)
    # teams
    teams_dir, teams_file = os.path.split(options.teams_file or constants.FNAME_TEAMS)
    teams_file_path = os.path.join(options.work_dir if work_dir_override else teams_dir, teams_file)
    # rooms
    rooms_dir, rooms_file = os.path.split(options.rooms_file or constants.FNAME_ROOMS)
    rooms_file_path = os.path.join(options.work_dir if work_dir_override else teams_dir, rooms_file)
    # teams_users
    teams_users_dir, teams_users_file = os.path.split(options.teams_users_file or constants.FNAME_TEAMS_USERS)
    teams_users_file_path = os.path.join(options.work_dir if work_dir_override else teams_dir, teams_users_file)

    # Teams
    if teams_file_path:
        teams_df = pd.read_csv(teams_file_path)
        teams_df.is_active.fillna(value=False, inplace=True)

        teams_df = filter_df(teams_df, options)

        if options.team_filter:
            t_filter = "|".join(options.team_filter.split(","))
            teams_df = teams_df[teams_df.team_name.str.contains(t_filter)]
        
        print(teams_df)

        teams_set = pd.Series(teams_df.team_name.unique()).to_list()
        teams_set_str = ",".join(teams_set)
    
        if options.create_teams:
            logging.info("Creating Teams")
            create_teams(teams_df)

        if options.delete_teams:
            delete_teams(teams_df)
    
    # Rooms
    if rooms_file_path:
        rooms_df = pd.read_csv(rooms_file_path)
        rooms_df.team_name.fillna(value=teams_set_str, inplace=True)
        rooms_df.is_active.fillna(value=False, inplace=True)
        # the "team_name" can be a list of team names
        rooms_df.team_name = rooms_df.team_name.str.split(",")
        rooms_df = rooms_df.explode("team_name")

        rooms_df = filter_df(rooms_df, options)

        if options.team_filter:
            t_filter = "|".join(options.team_filter.split(","))
            rooms_df = rooms_df[rooms_df.team_name.str.contains(t_filter)]
        
        if options.room_filter:
            r_filter = "|".join(options.room_filter.split(","))
            rooms_df = rooms_df[rooms_df.room_name.str.contains(m_filter)]

        print("Rooms config")
        print(rooms_df)
    
        if options.create_rooms:
            logging.info("Creating Rooms")
            create_rooms(rooms_df)

    # Teams users
    if teams_users_file_path:
        teams_users_df = pd.read_csv(teams_users_file_path)
        teams_users_df.team_name.fillna(value="", inplace=True)
        teams_users_df.mail_addr.fillna(value="", inplace=True)
        teams_users_df.is_active.fillna(value=False, inplace=True)
        teams_users_df.is_moderator.fillna(value=False, inplace=True)

        teams_users_df = filter_df(teams_users_df, options)

        print(teams_users_df)

        if options.assign_users:
            map_users_to_teams(teams_users_df)

    #await clean_msgs_rooms(rooms)
    
    # url_maps = await get_room_to_url_map(rooms)
    # print(tabulate(sorted(url_maps, key = lambda i: i["room"]), headers="keys"))

    # member_map = get_teams_membership(all_teams, filter=True)
    # print(tabulate(member_map, headers="keys"))

    return
        
if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    