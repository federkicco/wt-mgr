from webexteamssdk import WebexTeamsAPI
from webexteamssdk.exceptions import ApiError
from pprint import pprint
import argparse
import re

api = WebexTeamsAPI()


def team_mail_by_name(team_name):
    class_name = team_name.split()[-1]
    if "Medie" in team_name:
        prefix = "medie"
    elif "Elementari" in team_name:
        prefix = "elem"
    else:
        return

    return f"{prefix}_{class_name}@aurigatech.it".lower()


def add_mail_to_teams(teams_list)
    for team in teams_list:
        team_name = team.name
        e_mail = team_mail_by_name(team.name)
        pprint(f"Adding {e_mail} to team: {team_name}")
        try:
            api.team_memberships.create(team.id, personEmail=e_mail)
        except ApiError as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    teams_list = [ team for team in api.teams.list()]
    teams = { team.id: team.name for team in teams_list }
    rooms = [ room for room in api.rooms.list()]