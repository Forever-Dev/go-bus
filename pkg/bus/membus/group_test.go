package membus_test

import (
	"sync"

	"github.com/go-faker/faker/v4"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/forever-dev/go-bus/pkg/bus/membus"
)

var _ = Describe("Group", func() {
	Describe("NewGroups", func() {
		It("creates a new Groups instance", func() {
			groups := membus.NewGroups()
			Expect(groups).NotTo(BeNil())
			Expect(len(groups)).To(Equal(0))
		})
	})

	Describe("NewGroup", func() {
		It("creates a new Group instance", func() {
			id := faker.Word()
			group := membus.NewGroup(id)
			Expect(group).NotTo(BeNil())
			Expect(group.Id).To(Equal(id))
		})
	})

	Describe("AddMember", func() {
		It("adds a member to the group", func() {
			id := faker.Word()
			group := membus.NewGroup(id)

			memberId := faker.Word()

			err := group.AddMember(memberId, nil)
			Expect(err).To(Succeed())
			Expect(len(group.Members)).To(Equal(1))
			Expect(group.Members[memberId]).NotTo(BeNil())
		})

		It("should work concurrently", func() {
			id := faker.Word()
			group := membus.NewGroup(id)
			numMembers := 100

			errs := make(chan error, numMembers)

			for range numMembers {
				go func() {
					memberId := uuid.New().String()
					errs <- group.AddMember(memberId, nil)
				}()
			}

			for range numMembers {
				err := <-errs
				Expect(err).To(Succeed())
			}

			Expect(len(group.Members)).To(Equal(numMembers))
		})
	})

	Describe("ChooseMember", func() {
		It("balances member choices from the group", func() {
			id := faker.Word()
			group := membus.NewGroup(id)

			numMembers := 5

			for range numMembers - 1 {
				Expect(group.AddMember(uuid.New().String(), nil)).To(Succeed())
			}

			chosenMembers := make(map[string]struct{})
			for range numMembers - 1 {
				chosenMember := group.ChooseMember()
				Expect(chosenMember).NotTo(BeNil())
				_, exists := chosenMembers[chosenMember.Id]
				Expect(exists).To(BeFalse())
				chosenMembers[chosenMember.Id] = struct{}{}
			}

			chosenMember := group.ChooseMember()
			Expect(chosenMember).NotTo(BeNil())
			Expect(chosenMembers[chosenMember.Id]).NotTo(BeNil())
		})

		It("should work concurrently", func() {
			id := faker.Word()
			group := membus.NewGroup(id)

			numMembers := 10
			mu := sync.Mutex{}

			for range numMembers {
				Expect(group.AddMember(uuid.New().String(), nil)).To(Succeed())
			}

			numChoices := 100
			chosenMembers := make(chan string, numChoices)

			for range numChoices {
				go func() {
					chosenMember := group.ChooseMember()
					chosenMembers <- chosenMember.Id
				}()
			}

			memberCount := make(map[string]int)
			halfway := numChoices / 2
			for range halfway {
				go func() {
					memberId := <-chosenMembers
					mu.Lock()
					memberCount[memberId]++
					mu.Unlock()
				}()
			}

			for i := halfway; i < numChoices; i++ {
				memberId := <-chosenMembers
				mu.Lock()
				memberCount[memberId]++
				mu.Unlock()
			}

			Expect(len(memberCount)).To(Equal(numMembers))
		})
	})

	Describe("RemoveMember", func() {
		It("removes a member from the group", func() {
			id := faker.Word()
			group := membus.NewGroup(id)

			memberId := faker.Word()
			Expect(group.AddMember(memberId, nil)).To(Succeed())
			Expect(len(group.Members)).To(Equal(1))
			Expect(group.AddMember(faker.Word(), nil)).To(Succeed())
			Expect(len(group.Members)).To(Equal(2))

			group.RemoveMember(memberId)
			Expect(len(group.Members)).To(Equal(1))
		})
	})

	Context("finish processing", func() {
		It("marks a message as finished processing for a member", func() {
			id := faker.Word()
			group := membus.NewGroup(id)

			memberId := faker.Word()
			Expect(group.AddMember(memberId, nil)).To(Succeed())

			member := group.Members[memberId]
			chosenMember := group.ChooseMember()
			Expect(chosenMember).To(Equal(member))

			Expect(member.FinishProcessing()).To(Succeed())
			Expect(member.FinishProcessing()).NotTo(Succeed())
		})

		It("returns an error when finishing processing with no active messages", func() {
			id := faker.Word()
			group := membus.NewGroup(id)

			memberId := faker.Word()
			Expect(group.AddMember(memberId, nil)).To(Succeed())

			member := group.Members[memberId]

			err := member.FinishProcessing()
			Expect(err).ToNot(Succeed())
		})
	})
})
