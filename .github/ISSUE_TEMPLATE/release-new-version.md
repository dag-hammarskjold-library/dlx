---
name: Create new release
about: Tasks to complete for a new release
title: Create new release
labels: ''
assignees: ''

---

- [ ] Ensure all other issues in the milestone have been closed or removed
- [ ] Issue a PR updating the version to the new release version in setup.py
- [ ] Create a release from the tag, with the same name as the tag
- [ ] Issue a PR updating version to the new dev version (`<version>.dev`) in setup.py, closing this issue
- [ ] Close the milestone
