print("COllins: ", __name__)


class Teaching:
    @classmethod
    def conn_string(cls, names):
        fname, lname = names.split()
        return cls(fname=fname, lname=lname)

    def __init__(self, fname, lname):
        self.lname = lname
        print("collins")
        self.fname = fname


    def extract(self):
        print(f"{self.fname}: Extracting data from database")

    def fe(self):
        #self.lname = "omolewa"
        print(10 * 20)

    def model(self):
        print("Building the model")

    def api(self):
        print(f"{self.lname}: Creating the API")

    def run_pipeline(self):
        self.extract()
        self.fe()
        self.model()
        self.api()

        print(f"{self.fname}, {self.lname}: I am done with the pipeline")


#teach = Teaching.creating_variables("omolewa Adaramola")
# teach = Teaching()
# teach.run_pipeline()

def main():
    # teach = Teaching(fname="Omolewa", lname="Adaramola")
    # teach.run_pipeline()
    teach = Teaching.creating_variables("omolewa Adaramola")
    teach.run_pipeline()


if __name__ == "__main__":
    print("This is my name", __name__)
    main()