package memory

func (s *MessageStore) nextBoxIDLocked(userID int64) int {
	next := s.nextBox[userID] + 1
	s.nextBox[userID] = next
	return next
}

func (s *MessageStore) nextPtsLocked(userID int64) int {
	next := s.nextPts[userID] + 1
	s.nextPts[userID] = next
	return next
}

func (s *MessageStore) nextPtsNLocked(userID int64, count int) int {
	if count <= 0 {
		count = 1
	}
	next := s.nextPts[userID] + count
	s.nextPts[userID] = next
	return next
}
