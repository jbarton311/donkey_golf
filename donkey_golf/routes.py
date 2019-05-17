import logging
import pandas as pd

from flask import render_template, url_for, flash, redirect, request
from donkey_golf import app, db, bcrypt, config
from donkey_golf import data_things as dt
from donkey_golf.forms import RegistrationForm, LoginForm
from donkey_golf.models import User, Teams
from flask_login import login_user, current_user, logout_user, login_required

# create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)

conf = config.DGConfig()

@app.route("/")
@app.route("/home")
def home():
    return render_template('home.html')

@app.route("/home_v2")
def home_v2():
    return render_template('home_v2.html')

@app.route("/about")
def about():
    return render_template('about.html', title='About')


@app.route("/register", methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('home'))
    form = RegistrationForm()
    if form.validate_on_submit():
        # Take their password and hash it
        hashed_password = bcrypt.generate_password_hash(form.password.data).decode('utf-8')

        # Save their data to the db
        email = form.email.data.lower()
        user = User(username=form.username.data, email=email, password=hashed_password)
        db.session.add(user)
        db.session.commit()
        flash('Your account has been created! You are now able to log in', 'success')
        login_user(user)
        return render_template('home.html')
    return render_template('register.html', title='Register', form=form)


@app.route("/login", methods=['GET', 'POST'])
def login():
    # If already logged in, send them home
    if current_user.is_authenticated:
        return redirect(url_for('home'))

    form = LoginForm()
    if form.validate_on_submit():

        # All emails are stored in lower case
        # and will be validated with lower case
        email = form.email.data.lower()
        # Grab the user if it exists
        user = User.query.filter_by(email=email).first()
        # IF a user exists and their password matches, login them in
        if user and bcrypt.check_password_hash(user.password, form.password.data):
            login_user(user, remember=form.remember.data)

            # This is for when someone clicks a route before they are logged in
            # this will route them back to that route
            next_page = request.args.get('next')
            return redirect(next_page) if next_page else redirect(url_for('home'))
        else:
            flash('Login Unsuccessful. Please check email and password', 'danger')
    return render_template('login.html', title='Login', form=form)

@app.route("/logout")
def logout():
    logout_user()
    return redirect(url_for('home'))

@app.route("/account")
@login_required
def account():
    user_data = dt.UserData(user_id=current_user.id)
    user_data.run()
    user_df = user_data.data
    return render_template('account.html',
                            title='Account',
                            user_df=user_df)

@app.route("/my_team", methods=['GET', 'POST'])
@login_required
def my_team():
    logger.debug(f"CURRENT USER ID is {current_user.id}")
    # Pull users team
    leaderboard = dt.PullLeaderboard(user_id=current_user.id)
    leaderboard.pull_tourney_leaderboard()
    df_team_results = getattr(leaderboard, 'user_df', pd.DataFrame())
    tourney_status = leaderboard.tourney_status

    tourney_data = dt.TournamentInfo()
    tourney_data.pull_tourney_info()
    t_info = tourney_data.tourney_data

    user_data = dt.UserData(user_id=current_user.id)
    user_data.run()
    user_df = user_data.data
    # If they have a team, take them to their team
    if not df_team_results.empty:
        logger.warning("Taking them to their user page")
        return render_template('user_team.html',
                                title='My Team',
                               user_id=current_user.id,
                               user_df=user_df,
                               team=df_team_results,
                               t_info=t_info)

    # If they dont have a team and the tourney has started, sorry!
    if tourney_status != 'pre_tourney':
        return render_template('sorry_tourney_has_started.html')
    # If it's still pre tourney, let them draft
    else:
        draft = dt.DraftDay()
        draft.run()
        # Pull a list of available players and rankings
        lb_df = draft.data

        if request.method == 'POST':
            team_list = request.form.getlist('team_list')
            print(f'TEAM LIST: {team_list}')

            grouper = lb_df.loc[lb_df['player'].isin(team_list)].groupby(['tier'])['player'].count().reset_index()
            tier_dict = dict(zip(grouper.tier, grouper.player))
            tier_1 = tier_dict.get('Tier 1', 0)
            tier_2 = tier_dict.get('Tier 2', 0)
            tier_3 = tier_dict.get('Tier 3', 0)
            print(tier_dict)

            # Make sure they pick 3 people from each tier
            if tier_1 == 2 and tier_2 == 3 and tier_3 == 2:
                try:
                    for golfer in team_list:
                        entry = Teams(id=current_user.id,
                                      tourney_id=leaderboard.tourney_id,
                                      player=golfer)
                        db.session.add(entry)
                        db.session.commit()
                    flash('Congrats - you have selected a team!', 'success')
                    logger.warning("Taking them to their user page")

                    # Make sure there team displays correctly after they draft
                    leaderboard = dt.PullLeaderboard(user_id=current_user.id)
                    leaderboard.pull_tourney_leaderboard()
                    df_team_results = leaderboard.user_df

                    user_data = dt.UserData(user_id=current_user.id)
                    user_data.run()
                    user_df = user_data.data

                    return render_template('user_team.html',
                                            title='My Team',
                                           user_id=current_user.id,
                                           user_df=user_df,
                                           team=df_team_results,
                                           t_info=t_info)

                except Exception as e:
                    flash("Uh Oh - Weird Error", 'danger')
                    flash(f'{e}', 'info')
            else:
                flash('Pick the following:\n Tier A: 2\n Tier B: 3\n Tier C: 2, DUMMY!', 'danger')

    return render_template('available_players.html',
                            title='Players',
                            df=lb_df,
                             t_info=t_info)

@app.route("/how_to_play")
def how_to_play():
    return render_template('how_to_play.html', title='How To Play')

@app.route("/tourney_leaderboard")
@login_required
def tourney_leaderboard():

    leaderboard = dt.PullLeaderboard(user_id=current_user.id)
    leaderboard.run()
    df_tourney = leaderboard.data
    tourney_status = leaderboard.tourney_status

    tourney_data = dt.TournamentInfo()
    tourney_data.pull_tourney_info()
    t_info = tourney_data.tourney_data

    if tourney_status == 'pre_tourney':
        logger.info("We in pre tourney")
        leaderboard.pull_pre_tourney_data()

        return render_template('pre_tourney_leaderboard.html',
                               title='Tourney Leaderboard',
                               df_tourney=leaderboard.df_pre_tourney,
                               t_info=t_info)
    else:
        refresh = getattr(leaderboard, 'refresh_string', 'NO REFRESH')

        try:
            cut_dict = leaderboard.calculate_cut_line()
        except Exception as e:
            cut_dict = None

        return render_template('tourney_leaderboard.html',
                               title='Tourney Leaderboard',
                               df_tourney=df_tourney,
                               cut_dict=cut_dict,
                               t_info=t_info,
                               tourney_status=tourney_status,
                               refresh=refresh)

@app.route("/game_scoreboard")
@login_required
def game_scoreboard():
    scoreboard = dt.GameScoreboard()
    scoreboard.run()

    tourney_info = dt.TournamentInfo()
    tourney_info.pull_tourney_info()
    t_info = tourney_info.tourney_data

    if scoreboard.tourney_status == 'pre_tourney':
        return render_template('pre_tourney_scoreboard.html',
                                df=scoreboard.df_drafted_teams,
                                t_info=t_info)
    else:

        df_sb = scoreboard.data

        leaderboard = dt.PullLeaderboard(user_id=current_user.id)
        leaderboard.run()
        refresh = leaderboard.refresh_string
        df_tourney = leaderboard.data
        df_tourney = df_tourney.loc[df_tourney['team_count'] > 0]

        try:
            cut_dict = leaderboard.calculate_cut_line()
        except Exception as e:
            cut_dict = None

        tourney_status = leaderboard.tourney_status



        team_by_player = dt.TeamPlayerResults()
        team_by_player.run()
        team_player_df = team_by_player.data

        # Bring in scoreboard data to team results
        team_player_df = team_player_df.merge(df_sb, how='left', on='id',
              suffixes=('_team','_gs'))

        #team_player_df.sort_values('rank', inplace=True)              

        return render_template('game_scoreboard.html',
                               title='Donkey Leaderboard',
                               df_sb=df_sb,
                               df_tourney=df_tourney,
                               t_info=t_info,
                               tourney_status=tourney_status,
                               team_player_df=team_player_df,
                               cut_dict=cut_dict,
                               refresh=refresh)
