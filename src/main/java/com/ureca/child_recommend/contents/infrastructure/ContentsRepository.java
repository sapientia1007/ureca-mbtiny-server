package com.ureca.child_recommend.contents.infrastructure;

import com.ureca.child_recommend.contents.domain.Contents;
import com.ureca.child_recommend.contents.domain.Enum.ContentsStatus;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public interface ContentsRepository extends JpaRepository<Contents, Long> {

    Optional<Contents> findByTitleAndAuthor(String title, String author);

    List<Contents> findByStatusAndTitleContaining(ContentsStatus status, String title);

    List<Contents> findByIdIn(List<Long> contentsIdList);

    Optional<Contents> findByIdAndStatus(Long contentsId , ContentsStatus contentsStatus);


    @Query("SELECT c FROM Contents c WHERE c.title LIKE %:keyword% AND c.status = :status")
    List<Contents> findByTitleAndStatus(@Param("keyword") String keyword, @Param("status") ContentsStatus status);


    @EntityGraph(attributePaths = {"contentsMbti"})
    Optional<Contents> findWithContentsScoreById(Long contentId);


    // 특정 type에 따라 랜덤으로 15개 가져오기 - ACTIVE인 상태
    @Query(value = "SELECT * FROM contents WHERE contents_mbti_result = :type AND status = 'ACTIVE' ORDER BY RANDOM() LIMIT 15", nativeQuery = true)
    List<Contents> findRandomByContentsMbtiResult(@Param("type") String type);

    // 전체 콘텐츠에서 랜덤으로 15개 가져오기 - ACTIVE인 상태
    @Query(value = "SELECT * FROM contents WHERE status = 'ACTIVE' ORDER BY RANDOM() LIMIT 15", nativeQuery = true)
    List<Contents> findRandomContents();

    @Query("DELETE FROM Contents c WHERE c.status = :status AND c.updateAt <= :date")
    void deleteByStatusAndUpdatedAtBefore(@Param("status") ContentsStatus status, @Param("date") LocalDateTime date);

}
