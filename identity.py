"""Name each connected component of the valid user links."""


def build_identity(con, _scratch):
    parents: dict[str, str] = {}
    sizes: dict[str, int] = {}
    minimum: dict[str, str] = {}

    def find(user: str) -> str:
        while parents[user] != user:
            parents[user] = parents[parents[user]]
            user = parents[user]
        return user

    def add(user: str) -> None:
        if user not in parents:
            parents[user] = user
            sizes[user] = 1
            minimum[user] = user

    edges = con.execute("""
        SELECT user_id_a, user_id_b
        FROM raw_links
        WHERE user_id_a IS NOT NULL AND user_id_b IS NOT NULL
            AND user_id_a <> '' AND user_id_b <> ''
            AND user_id_a <> user_id_b
    """)
    batch_size = 50_000
    while batch := edges.fetchmany(batch_size):
        for left, right in batch:
            add(left)
            add(right)
            root_a = find(left)
            root_b = find(right)
            if root_a == root_b:
                continue
            if sizes[root_a] < sizes[root_b]:
                root_a, root_b = root_b, root_a
            parents[root_b] = root_a
            sizes[root_a] += sizes.pop(root_b)
            # UTF-8 preserves code point ordering for valid Unicode strings.
            minimum[root_a] = min(minimum[root_a], minimum.pop(root_b))

    con.execute("CREATE TEMP TABLE identity_map (user_id VARCHAR, canonical_id VARCHAR)")
    insert_sql = """
        INSERT INTO identity_map
        SELECT unnest(?::VARCHAR[]), unnest(?::VARCHAR[])
    """
    users = []
    names = []
    for user in parents:
        users.append(user)
        names.append(minimum[find(user)])
        if len(users) == batch_size:
            con.execute(insert_sql, [users, names])
            users.clear()
            names.clear()
    if users:
        con.execute(insert_sql, [users, names])
