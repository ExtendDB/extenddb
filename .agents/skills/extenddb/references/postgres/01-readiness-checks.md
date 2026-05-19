# PostgreSQL Readiness Checks

## 1. Purpose

Verify that PostgreSQL is running and reachable before `extenddb init`. Skip `extenddb init` until `pg_isready` exits 0.

## 2. Primary check

```bash
pg_isready -q
```

- Exit 0 means PostgreSQL is ready. Return to the setup flow.
- Exit nonzero means PostgreSQL is not ready. Continue with this file.

## 3. Start commands by platform

Linux (systemd):

```bash
sudo systemctl start postgresql
```

Check status:

```bash
sudo systemctl status postgresql
```

macOS (Homebrew):

```bash
brew services start postgresql@17
```

Check status:

```bash
brew services list
```

## 4. Secondary reachability check

If `pg_isready` passes but `extenddb init` still fails with connection errors, confirm that the user's local client can actually connect:

```bash
psql -c "SELECT 1" -U postgres
```

If this fails with authentication errors, see `references/02-from-scratch.md` for `pg_hba.conf` guidance. If this fails with "role postgres does not exist" on macOS, use the following command because Homebrew creates the superuser as the current user:

```bash
psql -c "SELECT 1" -U $(whoami)
```

## 5. Port check

If PostgreSQL is running but on a nonstandard port:

```bash
pg_isready -q -p 5433
```

Adjust the port to match the user's configuration.

## 6. Remote PostgreSQL or Aurora

`pg_isready` accepts `-h` for remote hosts:

```bash
pg_isready -q -h <hostname> -p 5432
```

For RDS and Aurora, use the endpoint hostname. The secondary check needs the password:

```bash
PGPASSWORD=<password> psql -c "SELECT 1" -h <hostname> -U <user>
```

## 7. Common failure causes

- PostgreSQL installed but not started. See Section 3.
- PostgreSQL running on a nonstandard port. See Section 5.
- Firewall blocks 5432. Check `sudo firewall-cmd --list-ports` on Fedora or RHEL, or `sudo ufw status` on Ubuntu.
- Remote PostgreSQL not accepting TCP connections. Check `listen_addresses` in `postgresql.conf`.
