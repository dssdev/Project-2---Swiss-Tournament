#!/usr/bin/env python
# 
# tournament.py -- implementation of a Swiss-system tournament
#

import psycopg2


def connect():
    """Connect to the PostgreSQL database.  Returns a database connection."""
    return psycopg2.connect("dbname=tournament")


def deleteMatches():
    """Remove all the match records from the database."""
    conn = connect()
    conn.cursor().execute('delete from match;')
    conn.commit()
    conn.close()



def deletePlayers():
    """Remove all the player records from the database."""
    conn = connect()
    conn.cursor().execute('delete from player;')
    conn.commit()
    conn.close()

def countPlayers():
    """Returns the number of players currently registered."""
    conn = connect()
    cur = conn.cursor()
    cur.execute('select count(*) from player;')
    row = cur.fetchone();
    count = row[0]
    conn.commit()
    conn.close()
    return count

def registerPlayer(name):
    """Adds a player to the tournament database.
  
    The database assigns a unique serial id number for the player.  (This
    should be handled by your SQL database schema, not in your Python code.)
  
    Args:
      name: the player's full name (need not be unique).
    """
    conn = connect()
    conn.cursor().execute('insert into player(name) VALUES (%s);',(name,))
    conn.commit()
    conn.close()

def playerStandings():
    """Returns a list of the players and their win records, sorted by wins.

    The first entry in the list should be the player in first place, or a player
    tied for first place if there is currently a tie.

    Returns:
      A list of tuples, each of which contains (id, name, wins, matches):
        id: the player's unique id (assigned by the database)
        name: the player's full name (as registered)
        wins: the number of matches the player has won
        matches: the number of matches the player has played
    """
    conn = connect()
    cur = conn.cursor()
    cur.execute("""select pid,
                          name,
                          sum(case WHEN pid=winner THEN 1
                                   ELSE 0
                                   END),
                          count(winner)
                   from player left join match on pid=winner or pid=loser group by pid;""")
    standings = [(row[0],row[1],int(row[2]),int(row[3])) for row in cur.fetchall()]
    print(standings)
    conn.commit()
    conn.close()
    return standings

def reportMatch(winner, loser):
    """Records the outcome of a single match between two players.

    Args:
      winner:  the id number of the player who won
      loser:  the id number of the player who lost
    """
    conn = connect()
    conn.cursor().execute('insert into match(winner,loser) VALUES (%s,%s);',(winner,loser))
    conn.commit()
    conn.close()
 
def swissPairings():
    """Returns a list of pairs of players for the next round of a match.
  
    Assuming that there are an even number of players registered, each player
    appears exactly once in the pairings.  Each player is paired with another
    player with an equal or nearly-equal win record, that is, a player adjacent
    to him or her in the standings.
  
    Returns:
      A list of tuples, each of which contains (id1, name1, id2, name2)
        id1: the first player's unique id
        name1: the first player's name
        id2: the second player's unique id
        name2: the second player's name
    """
    conn = connect()
    cur = conn.cursor()
    cur.execute("""select pid,
                          name,
                          sum(case WHEN pid=winner THEN 1
                                   ELSE 0
                                   END) as wins
                   from player left join match on pid=winner or pid=loser group by pid order by wins desc;""")
    pairings = []
    count = 1
    player1 = ()
    player2 = ()
    for row in cur.fetchall():
        if count%2 != 0:
            player1 = (row[0],row[1])
        else:
            player2 = (row[0],row[1])
            pairings.append(player1 + player2)
        count +=1


    print(pairings)
    conn.commit()
    conn.close()
    return pairings

