ua_df = pd.read_csv("useragent.csv")
ua_df["string"] = ua_df["string"].fillna("")
ua_df["id"] = ua_df.index
uaf_df = db.get_useragent_family_df(dbc)
uaf_df = uaf_df.rename(columns={"id": "family_id"})
ua_df = ua_df[["string", "family", "id"]]
merged = ua_df.merge(uaf_df[["family_id", "family"]], how="left", on="family")
merged = merged[["id", "string", "family_id"]]
merged.to_csv("useragent_df.csv", index=False)
