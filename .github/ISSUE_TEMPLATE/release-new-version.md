---
name: Create new release
about: Tasks to complete for a new release
title: Create new release
labels: ''
assignees: ''

---

- [ ] Ensure all other issues in the milestone have been closed or removed
- [ ] Issue a PR updating the version to the new release version in setup.py
- [ ] Create a new release, named by the version and creating a new tag with the same name
- [ ] Issue a PR updating the version to the new dev version (`<version>.dev`) in setup.py, closing this issue
