# Dedup Findings & Known Limitations

## What we built
PySpark fuzzy dedup using last-name blocking + Levenshtein distance
on first name + email prefix + state match.

## What went wrong
1. Last-name blocking failed ~50% of the time because slightly_mutate()
   was called on last names in the generator — duplicate pairs ended up
   in different blocking groups and were never compared.

2. Email was rebuilt from scratch for duplicate records instead of being
   mutated — giving edit distances of 10+ on full usernames. Our threshold
   of 2 never caught these.

## What I learned
- Evaluation logic bugs are as dangerous as model bugs.
- Blocking strategy must be designed around the actual data.
- In production I would use Splink for probabilistic record linkage
  instead of hand-tuned thresholds.

## Fix (not implemented — moving on)
Change generator to mutate email slightly instead of rebuilding it,
and never mutate last name since it is the blocking key.
