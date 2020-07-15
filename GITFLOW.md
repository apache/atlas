# Git flow

## Sync from origin repo
```bash
git fetch upstream

# Sync master branch
git checkout master
git merge upstream/master
git push origin master

# Sync any branch
git checkout ${branch}
git merge upstream/${branch}
git push origin ${branch}
```

## Upgrade Atlas version
```bash
git checkout heetch-atlas-2.0
git checkout -b heetch-atlas-${tag}
git rebase ${tag}
... fix rebase ...
git push orign heetch-atlas-${tag}
```

## Changelog
```bash
git log --pretty=oneline --abbrev-commit heetch-atlas-2.0..heetch-atlas-release-2.1.0-rc3 > changelog.txt
```