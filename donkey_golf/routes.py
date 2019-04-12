from flask import render_template, url_for, flash, redirect, request
from donkey_golf import app, db, bcrypt, data_utils, config
from donkey_golf.forms import RegistrationForm, LoginForm
from donkey_golf.models import User, Teams
from flask_login import login_user, current_user, logout_user, login_required

conf = config.DGConfig()

@app.route("/")
@app.route("/home")
def home():
    return render_template('home.html')

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
        user = User(username=form.username.data, email=form.email.data, password=hashed_password)
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
        # Grab the user if it exists
        user = User.query.filter_by(email=form.email.data).first()
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
    return render_template('account.html', title='Account')

@app.route("/my_team", methods=['GET', 'POST'])
@login_required
def my_team():

    # Pull users team
    df_team_results = data_utils.users_team(current_user.id)

    # If they have a team, take them to their team
    if not df_team_results.empty:
        return render_template('user_team.html', title='My Team',
                               user_id=current_user.id, team=df_team_results)

    # Pull a list of available players and rankings
    lb_df = data_utils.pull_available_players()

    if request.method == 'POST':
        team_list = request.form.getlist('team_list')
        print(f'TEAM LIST: {team_list}')

        grouper = lb_df.loc[lb_df['player'].isin(team_list)].groupby(['tier'])['player'].count().reset_index()
        tier_dict = dict(zip(grouper.tier, grouper.player))
        tier_1 = tier_dict.get('Tier 1', 0)
        tier_2 = tier_dict.get('Tier 2', 0)
        print(tier_dict)

        # Make sure they pick 3 people from each tier
        if tier_1 == 3 and tier_2 == 3:
            print('Clutch')
            print(conf.tourney_id)
            print(team_list)
            try:
                for golfer in team_list:
                    entry = Teams(id=current_user.id,
                                  tourney_id=conf.tourney_id,
                                  golfer=golfer)
                    db.session.add(entry)
                    db.session.commit()
                flash('Congrats - you have selected a team!', 'success')
            except Exception as e:
                flash("Uh Oh - Weird Error", 'danger')
                flash(f'{e}', 'info')
        else:
            flash('Pick exactly 3 from each tier, DUMMY!', 'danger')

    return render_template('available_players.html', title='Players',
                            df=data_utils.pull_available_players())

@app.route("/how_to_play")
def how_to_play():
    return render_template('how_to_play.html', title='How To Play')

@app.route("/tourney_leaderboard")
@login_required
def tourney_leaderboard():

    data_utils.data_load_leaderboard()
    df_tourney = data_utils.pull_tourney_leaderboard(user_id=current_user.id)
    #df_info = pull_tourney_info()
    #df_tiger = tiger_results(user_id=current_user.id)

    try:
        cut_dict = data_utils.calculate_cut_line()
    except Exception as e:
        cut_dict = None

    return render_template('tourney_leaderboard.html',
                           title='Tourney Leaderboard',
                           df_tourney=df_tourney,
                           cut_dict=cut_dict)

@app.route("/game_scoreboard")
@login_required
def game_scoreboard():
    df_sb = data_utils.pull_scoreboard()
    return render_template('game_scoreboard.html',
                           title='Donkey Leaderboard',
                           df_sb=df_sb)
