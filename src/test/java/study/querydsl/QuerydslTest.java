package study.querydsl;

import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.Team;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceUnit;

import java.util.List;

import static com.querydsl.jpa.JPAExpressions.*;
import static org.assertj.core.api.Assertions.assertThat;
import static study.querydsl.entity.QMember.*;
import static study.querydsl.entity.QTeam.team;

@Transactional
@SpringBootTest
public class QuerydslTest {

    @Autowired
    EntityManager em;

    JPAQueryFactory queryFactory;

    @BeforeEach
    public void before() {
        queryFactory = new JPAQueryFactory(em);
        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);

        Member member1 = new Member("member1",10,teamA);
        Member member2 = new Member("member2",15,teamA);
        Member member3 = new Member("member3",20,teamB);
        Member member4 = new Member("member4",25,teamB);
        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);
    }

    // JPQL
    @Test
    public void startJPQL() {
        // member1 을 찾아라
        String qlString =
                "select m from Member m" +
                "where m.username = :username";
        Member findMember = em.createQuery(qlString, Member.class)
                .setParameter("username", "member1")
                .getSingleResult();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    // Querydsl
    @Test
    public void startQuerydsl() {
        // member1 을 찾아라
        Member findMember = queryFactory
                .select(member)
                .from(member)
                .where(member.username.eq("member1"))    // 파라미터 바인딩 처리
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    // 검색 조건 쿼리
    @Test
    public void search(){
        Member findMember = queryFactory
                .selectFrom(member)      // .select.from 합칠 수 있음
                .where(member.username.eq("member1")
                        .and(member.age.eq(10)))    // and 로 검색조건 추가 가능
                .fetchOne();
        assertThat(findMember.getUsername()).isEqualTo("member1");
        assertThat(findMember.getAge()).isEqualTo(10);
    }
    // And 조건 쉼표 연결
    @Test
    public void searchAnd(){
        Member findMember = queryFactory
                .selectFrom(member)      // .select.from 합칠 수 있음
                .where(member.username.eq("member1")
                        ,member.age.eq(10))   // and 빼고 , 로 이을 수 있음
                .fetchOne();
        assertThat(findMember.getUsername()).isEqualTo("member1");
        assertThat(findMember.getAge()).isEqualTo(10);
    }

    // Fetch 결과 조회
    @Test
    public void resultFetch() {
        // 1.fetch -> 멤버의 목록을 리스트로 조회
        List<Member> fetch = queryFactory
                .selectFrom(member)
                .fetch();

        // 2. fetchOne -> 멤버 단건 조회
        Member fetchOne = queryFactory
                .selectFrom(member)
                .fetchOne();

        // 3. fetchFirst -> limit(1).fetchOne()
        Member fetchFirst = queryFactory
                .selectFrom(member)
                .fetchFirst();
    }

    // count 쿼리
    @Test
    public void count() {
        Long totalCount = queryFactory
                //.select(Wildcard.count) //select count(*)
                .select(member.count()) //select count(member.id)
                .from(member)
                .fetchOne();
        System.out.println("totalCount = " + totalCount);
    }

    //정렬
    /**
     * 회원 정렬 순서
     * 1. 회원 나이 내림차순(desc)
     * 2. 회원 나이 올림차순(asc)
     * 단 2에서 회원 이름이 없으면 마지막에 출력(nulls last)
     */
    @Test
    public void sort() {
        em.persist(new Member(null,100));
        em.persist(new Member("member5",100));
        em.persist(new Member("member6",100));  // 테스트를 위해 데이터 추가

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.eq(100))
                .orderBy(member.age.desc(), member.username.asc().nullsLast())
                .fetch();

        // 검증
        Member member5 = result.get(0);
        Member member6 = result.get(1);
        Member memberNull = result.get(2);
        assertThat(member5.getUsername()).isEqualTo("member5");
        assertThat(member6.getUsername()).isEqualTo("member6");
        assertThat(memberNull.getUsername()).isNull();
    }

    // 조회 건수 제한
    @Test
    public void paging() {
        List<Member> result = queryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1)  //0부터 시작(zero index)
                .limit(2)   //최대 2건 조회
                .fetch();

        assertThat(result.size()).isEqualTo(2);
    }

    // 전체 조회수가 필요할 때
    @Test
    public void paging2() {
        QueryResults<Member> queryResults = queryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1)
                .limit(2)
                .fetchResults();
        assertThat(queryResults.getTotal()).isEqualTo(4);
        assertThat(queryResults.getLimit()).isEqualTo(2);
        assertThat(queryResults.getOffset()).isEqualTo(1);
        assertThat(queryResults.getResults().size()).isEqualTo(2);
    }

    // 집합

    /**
     * JPQL
     * select
     * COUNT(m), //회원수
     * SUM(m.age), //나이 합
     * AVG(m.age), //평균 나이
     * MAX(m.age), //최대 나이
     * MIN(m.age) //최소 나이
     * from Member m
     */
    @Test
    public void aggregation() {
        List<Tuple> result = queryFactory
                .select(
                        member.count(),
                        member.age.sum(),
                        member.age.avg(),
                        member.age.max(),
                        member.age.min()
                )
                .from(member)
                .fetch();

        Tuple tuple = result.get(0);
        assertThat(tuple.get(member.count())).isEqualTo(4);
        assertThat(tuple.get(member.age.sum())).isEqualTo(70);
        assertThat(tuple.get(member.age.avg())).isEqualTo(17.5);
        assertThat(tuple.get(member.age.max())).isEqualTo(25);
        assertThat(tuple.get(member.age.min())).isEqualTo(10);
    }

    // 집합 - groupBy

    /**
     * 팀의 이름과 각 팀의 평균 연령을 구해라.
     */
    @Test
    public void group() throws Exception {
        List<Tuple> result = queryFactory
                .select(team.name, member.age.avg())
                .from(member)
                .join(member.team, team)
                .groupBy(team.name)     // 팀의 이름으로 그룹핑
                .fetch();
        Tuple teamA = result.get(0);
        Tuple teamB = result.get(1);    // 팀이 A,B 두개이니까 groupBy 결과도 2개
        assertThat(teamA.get(team.name)).isEqualTo("teamA");
        assertThat(teamA.get(member.age.avg())).isEqualTo(12.5);
        assertThat(teamB.get(team.name)).isEqualTo("teamB");
        assertThat(teamB.get(member.age.avg())).isEqualTo(22.5);
    }

    // 조인 (기본조인)

    /**
     * 팀 A에 소속된 모든 회원
     */
    @Test
    public void join() {
        List<Member> result = queryFactory
                .selectFrom(member)
                .join(member.team, team)
                .where(team.name.eq("teamA"))
                .fetch();

        assertThat(result)
                .extracting("username")
                .containsExactly("member1", "member2");
    }

    /**
     * 세타 조인(연관관계가 없는 필드로 조인)
     * 회원의 이름이 팀 이름과 같은 회원 조회
     */
    @Test
    public void theta_join() {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        List<Member> result = queryFactory
                .select(member)
                .from(member, team)
                .where(member.username.eq(team.name))
                .fetch();
        assertThat(result)
                .extracting("username")
                .containsExactly("teamA", "teamB");
    }

    // on 절 조인
    /**
     * 예) 회원과 팀을 조회하면서, 팀 이름이 teamA인 팀만 조인, 회원은 모두 조회
     * JPQL: select m,t from Member m left join m.team t on t.name = 'teamA'
     */
    @Test
    public void join_on_filtering() throws Exception {
        List<Tuple> result = queryFactory
                .select(member, team)
                .from(member)
                .join(member.team, team)
//                .on(team.name.eq("teamA"))
                .where(team.name.eq("teamA"))
                .fetch();
        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }
    /**
     * 연관관계가 없는 엔티티 외부 조인
     * 회원의 이름이 팀 이름과 같은 대상 외부 조인
     */
    @Test
    public void join_on_no_relation() {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        List<Tuple> result = queryFactory
                .select(member,team)
                .from(member)
                .leftJoin(team).on(member.username.eq(team.name))
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }




    // 페치 조인
    @PersistenceUnit
    EntityManagerFactory emf;

    @Test
    public void fetch_join() {
        em.flush();
        em.clear();

        Member findMember = queryFactory
                .selectFrom(member)
                .join(member.team,team).fetchJoin()         // 멤버와 연관된 팀까지 모두 한꺼번에 가져옴
                .where(member.username.eq("member1"))
                .fetchOne();

        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());   // 팀까지 가져왔으니 True 나와야함
        assertThat(loaded).as("패치 조인 적용").isTrue();
    }

    // 서브 쿼리
    /**
     * 나이가 가장 많은 회원을 조회 (eq)
     */
    @Test
    public void sub_query() {

        // 서브 쿼리용 Q멤버 하나 더 생성
      QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.eq(
                        // 서브쿼리에서는 JPAExpressions 사용 (지금은 static import 해놓음)
                        select(memberSub.age.max())    // 여기들어가는 멤버랑 바깥쪽 쿼리 멤버는 달라야함
                                .from(memberSub)    // select 절에서 멤버 다시 조회
                ))
                .fetch();

        assertThat(result).extracting("age")
                .containsExactly(25);
    }

    /**
     * 나이가 평균 이상인 회원 조회 (goe)
     */
    @Test
    public void sub_query_goe() {       // goe: greater or equal
        // 서브 쿼리용 Q멤버 하나 더 생성
        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.goe(
                        // 서브쿼리에서는 JPAExpressions 사용
                        select(memberSub.age.avg())    // 여기들어가는 멤버랑 바깥쪽 쿼리 멤버는 달라야함
                                .from(memberSub)    // select 절에서 멤버 다시 조회
                ))
                .fetch();

        assertThat(result).extracting("age")
                .containsExactly(20,25);
    }

    /**
     * 나이가 10살 초과인 사람 조회(in,gt)
     */
    @Test
    public void sub_query_in() {
        // 서브 쿼리용 Q멤버 하나 더 생성
        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.in(
                        // 서브쿼리에서는 JPAExpressions 사용
                        select(memberSub.age)    // 여기들어가는 멤버랑 바깥쪽 쿼리 멤버는 달라야함
                                .from(memberSub)    // select 절에서 멤버 다시 조회
                                .where(memberSub.age.gt(10))
                ))
                .fetch();

        assertThat(result).extracting("age")
                .containsExactly(15,20,25);
    }

    /**
     * where 절 말고 select 절에 서브쿼리
     */
    @Test
    public void select_sub_query(){
        // 서브 쿼리용 Q멤버 하나 더 생성
        QMember memberSub = new QMember("memberSub");

        List<Tuple> fetch = queryFactory
                .select(member.username,
                        select(memberSub.age.avg())
                                .from(memberSub)
                ).from(member)
                .fetch();
        for (Tuple tuple : fetch) {
            System.out.println("username = " + tuple.get(member.username));
            System.out.println("age = " +
                    tuple.get(select(memberSub.age.avg())
                            .from(memberSub)));
        }
    }

    // simple Case 문
    @Test
    public void simpleCase() {
        List<String> result = queryFactory
                .select(member.age
                        .when(10).then("열살")
                        .when(20).then("스무살")
                        .otherwise("기타"))
                .from(member)
                .fetch();
        for(String s: result){
            System.out.println("s = " + s);
        }
    }

    // 복잡한 case문
    @Test
    public void complexCase(){
        List<String> result = queryFactory
                .select(new CaseBuilder()
                        .when(member.age.between(0, 20)).then("0~20살")
                        .when(member.age.between(21, 30)).then("21~30살")
                        .otherwise("기타"))
                .from(member)
                .fetch();

        for(String s: result){
            System.out.println("s = " + s);
        }
    }

    // 상수가 필요할 때
    @Test
    public void constant() {
        List<Tuple> result = queryFactory
                .select(member.username, Expressions.constant("A")) // 결과에 상수가 포함돼서 나옴
                .from(member)
                .fetch();

        for (Tuple tuple:result){
            System.out.println("tuple = " + tuple);
        }
    }

    // 문자 더하기
    @Test
    public void concat(){

        // {username}_{age} 하고싶음
        List<String> result = queryFactory
                .select(member.username.concat("_").concat(member.age.stringValue()))
                .from(member)
                .where(member.username.eq("member1"))
                .fetch();

        for (String s: result){
            System.out.println("s = " + s);
        }
    }

}

