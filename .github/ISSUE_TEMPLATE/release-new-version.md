---
name: Release new version
about: Tasks to complete for a new release
title: Release version
labels: ''
assignees: ''

---

- [ ] create new branch 
- [ ] update version in `setup.py` in the new branch
- [ ] create tag from the new branch, named by the version
- [ ] create release from the tag, named by the version
- [ ] update the version in `main` to <version>.dev
- [ ] close milestone
- [ ] close this issue
