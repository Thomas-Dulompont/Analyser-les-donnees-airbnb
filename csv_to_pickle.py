import pandas as pd
import pickle

country_list = {
    "France":["Bordeaux","Lyon","Paris","Pays_Basque"],
    "belgique":["Antwerp","Brussels","Ghent"],
    "Netherlands":["Amsterdam","Rotterdam","The_Hague"],
    "United_Kingdom":["Bristol","Edinburgh","London","greater_manchester"],
}

for pays in country_list:
    for ville in country_list[pays]:
        pays = pays.lower()
        ville = ville.lower()

        listing = pd.read_csv(f'./data/{pays}/{ville}/listings-clean.csv')
        reviews = pd.read_csv(f'./data/{pays}/{ville}/reviews.csv')
        listings_brut = pd.read_csv(f'./data/{pays}/{ville}/listings-brut.csv')

        #########
        #########
        #########
        # QUESTION N°1

        quartiers = listing.groupby(by="neighbourhood_cleansed")

        # Nombre de review
        nb_review = quartiers["number_of_reviews"].sum()

        # Nombre de host
        nb_host = quartiers['id'].count()
        question1 = pd.DataFrame({'nb_host': nb_host, 'nb_review': nb_review})

        #########
        #########
        #########
        # QUESTION N°2

        # Taux de reponse moyen
        moyenne_response = listing["host_response_rate"].str.rstrip('%').astype(float).mean()

        # Taux acceptation moyen
        moyenne_accept = listing["host_acceptance_rate"].str.rstrip('%').astype(float).mean()

        question2 = pd.DataFrame({'mean_response': [moyenne_response], 'mean_accept': [moyenne_accept]}, index=["% par host"])


        # QUESTION N°3

        # Pourcentage type vérification (phone, work_email, email)
        phone_verification = listing["host_verifications"].apply(lambda x: 1 if "phone" in x else 0).sum() / listing["host_verifications"].count() * 100
        work_email_verification = listing["host_verifications"].apply(lambda x: 1 if "work_email" in x else 0).sum() / listing["host_verifications"].count() * 100
        email_verification = listing["host_verifications"].apply(lambda x: 1 if "email" in x and "work_email" != x else 0).sum() / listing["host_verifications"].count() * 100

        question3 = pd.DataFrame({'phone_verification': [phone_verification], 'work_email_verification': [email_verification], 'email_verification' : [email_verification]}, index=["% par host"])



        # QUESTION N°4

        # Moyenne d'amenities & écart type
        amenities = listing['amenities'].apply(lambda x: str(x)[1:-1].replace('"', '').replace("\\u2013", "-")).str.split(pat=",")
        listing['amenities'] = amenities

        listing["nb_amenities"] = listing["amenities"].apply(lambda x: len(x))

        g_room_type = listing[["room_type","nb_amenities"]].groupby(['room_type']).agg(['mean', 'std'])


        # QUESTION N°5
        listing["price"] = listing["price"].apply(lambda x: str(x).replace(",", "").replace("$", "")).astype(float)

        prix = listing[["room_type","price"]].groupby(["room_type"]).describe()
        prix = prix['price'].drop(['count', 'std', 'mean'], axis=1)


        # QUESTION N°6

        # On transform nos cases en string
        bathrooms = listing["bathrooms_text"]
        nb_bathrooms = listing["bathrooms"]

        # Ajoute "1" aux 3 cases qui n'ont pas de chiffres
        bathrooms = bathrooms.apply(lambda x: "1 {}".format(x) if x == "Shared half-bath" or x == "Half-bath" or x == "Private half-bath" else x).apply(lambda x: str(x)+"s" if str(x)[len(str(x))-1].lower() != "s" else str(x)).replace("nans", 0)

        # On sépare le chiffre du Type de bathroom
        bathrooms = bathrooms.str.split(' ', 1, expand=True)

        # On renome les colonnes
        bathrooms = bathrooms.rename(columns={0: "Nombre", 1: "Type"})

        # On tranform les str en float de la colonne "Nombre"
        bathrooms["Nombre"] = bathrooms["Nombre"].astype(float)

        def convertiseur(df):
            """
            Fonction qui multiplie les chiffres de la première colonne en fonction de la deuxième colonne
            : param df : DataFrame
            : return : DataFrame modifié
            """
            result = 0.0
            df[1] = str(df[1]).lower()
            if df[1] == "shared baths" or df[1] == "bath shareds" or df[1] == "half-baths":
                result=df[0]*0.5
            elif df[1] == "private baths":
                result=df[0]*2
            elif df[1] == "half-shareds" or df[1] == "shared half-baths":
                result=df[0]*0.25
            else :
                result = df[0]*1
            return result

        bathrooms["Nombre"] = bathrooms.apply(convertiseur,axis=1)
        bathrooms = bathrooms.groupby("Type").value_counts().reset_index().rename(columns={0:"count"}).drop("Type", axis=1).groupby("Nombre").sum()
        bathrooms = bathrooms.groupby("Nombre").sum().reset_index()

        # QUESTION N°7

        listing['len_description'] = listing['description'].apply(lambda x: len(str(x)))


        corr = listing['len_description'].corr(listing['number_of_reviews'])


        # QUESTION N°8

        fake_reviews = listings_brut.merge(reviews, left_on='id', right_on='listing_id')
        fake_reviews = fake_reviews[["host_name","host_id", "reviewer_name", "reviewer_id"]].loc[fake_reviews["host_name"] == fake_reviews["reviewer_name"]].drop_duplicates()

        annonces_fake = (len(fake_reviews) / len(reviews)) * 100
        annonces_fake


        reponses = {
            "1" : question1,
            "2" : question2,
            "3" : question3,
            "4" : g_room_type,
            "5" : prix,
            "6" : bathrooms,
            "7" : corr,
            "8" : annonces_fake
        }


        pickle_out = open(f"./data/pickle/{pays}_{ville}.pkl", "wb")
        pickle.dump(reponses, pickle_out)
        pickle_out.close()
