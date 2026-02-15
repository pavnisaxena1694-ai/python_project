✅ STEP 1 — Install Python 3.11 (Stable Version)

Go to official Python website

Download Python 3.11 (64-bit)

During installation:

✔ Check Add Python to PATH

✔ Choose “Install for all users” (if available)

After installation, open CMD and run:

py -3.11 --version


It should show:

Python 3.11.x


If yes → continue.

✅ STEP 2 — Create Clean Project Folder

Create a new folder:

C:\kaggle_project


Open CMD inside that folder:

cd C:\kaggle_project

✅ STEP 3 — Create Virtual Environment

Run:

py -3.11 -m venv venv


Now activate it:

venv\Scripts\activate


You should now see:

(venv) C:\kaggle_project>


That means you're inside isolated Python 3.11.

✅ STEP 4 — Install Kaggle Inside This Environment

Run:

pip install kaggle


After install:

kaggle --version


It should print Kaggle version.
If yes → installation problem solved.

✅ STEP 5 — Set Kaggle API Token (Environment Variable Method)

You already generated token from Kaggle.

Now set it.

Open normal CMD (not inside venv) and run:

setx KAGGLE_API_TOKEN "PASTE_YOUR_TOKEN_HERE"


Close CMD completely.

✅ STEP 6 — Reopen CMD → Activate venv Again
cd C:\kaggle_project
venv\Scripts\activate


Test token:

echo %KAGGLE_API_TOKEN%


If token appears → good.

✅ STEP 7 — Test Kaggle API

Run:

kaggle competitions list


If it prints competitions → DONE.

If "kaggle not recognized"

Run:

python -m kaggle competitions list
